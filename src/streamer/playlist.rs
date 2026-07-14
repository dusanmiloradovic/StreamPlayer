use crate::streamer::mixer::Mixer;
use crate::streamer::utils::{f_fadein_linear, f_fadein_log, f_fadeout_linear, f_fadeout_log};
use crate::streamer::{
    Callback, ControlCommand, ControlHandle, StreamErr, Streamer, StreamerCallBackHandle,
};
use async_broadcast::Receiver;
use crossbeam_channel::Sender;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Debug, PartialEq)]
pub enum CrossFadeType {
    Linear(f32),
    None,
    Logarithmic(f32),
}

pub struct PlayListStreamer {
    streamers: Arc<Mutex<Vec<Box<dyn Streamer>>>>, //looping in child thread and removing elements from vec
    control: ControlHandle,
    control_rx: Option<crossbeam_channel::Receiver<ControlCommand>>,
    sync_tx: Option<Sender<usize>>,
    callback_receiver: Option<Receiver<Callback>>,
    command_rx: Option<crossbeam_channel::Receiver<ControlCommand>>,
    callbacks: Arc<Mutex<HashMap<u64, Box<dyn Fn() + Send>>>>,
    cross_fade_type: CrossFadeType,
}

impl PlayListStreamer {
    pub fn new(streamers: Vec<Box<dyn Streamer>>, cross_fade_type: CrossFadeType) -> Self {
        let (control, control_rx) = ControlHandle::new();
        Self {
            streamers: Arc::new(Mutex::new(streamers)),
            cross_fade_type,
            control,
            control_rx: Some(control_rx),
            sync_tx: None,
            callback_receiver: None,
            command_rx: None,
            callbacks: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

fn loop_no_crossfade(
    streamer_queue: Arc<Mutex<Vec<Box<dyn Streamer>>>>,
    sender: SyncSender<Vec<f32>>,
    callback_receiver: Receiver<Callback>,
    callback_register: SyncSender<u64>,
) {
    let current = {
        let mut streamers = streamer_queue.lock().unwrap();
        if streamers.len() == 0 {
            return;
        }
        streamers.remove(0)
    };

    let sender_clone = sender.clone();
    let callback_receiver_clone = callback_receiver.clone();
    let callback_register_clone = callback_register.clone();
    let inital_streamers = vec![current];
    let mut mixer = Mixer::new(inital_streamers, vec![1]);
    let mixer_thread = mixer.play(sender, callback_receiver, callback_register);
    if let Err(some) = mixer_thread.join().unwrap() {
        println!("Error: {:?}", some);
        return; // TODO work on the error type return from joinhandle
    }

    loop_no_crossfade(
        streamer_queue.clone(),
        sender_clone,
        callback_receiver_clone,
        callback_register_clone,
    )
}

type FadeFn = Box<dyn Fn(usize) -> f32 + Send + Sync>;

struct FadeConfig {
    cutoff_duration: u64,
    fade_in: FadeFn,
    fade_out: FadeFn,
}

fn loop_crossfade(
    streamer_queue: Arc<Mutex<Vec<Box<dyn Streamer>>>>,
    sender: SyncSender<Vec<f32>>,
    callback_receiver: Receiver<Callback>,
    callback_register: SyncSender<u64>,
    fade_config: Arc<FadeConfig>,
    initial: bool,
) {
    let current = {
        let mut streamers = streamer_queue.lock().unwrap();
        if streamers.is_empty() {
            return;
        }
        streamers.remove(0)
    };

    let fade_config_clone = fade_config.clone();
    let f: FadeFn = {
        if initial {
            Box::new(move |x| (fade_config_clone.fade_out)(x))
        } else {
            Box::new(move |x| (fade_config_clone.fade_in)(x) * (fade_config_clone.fade_out)(x))
            // ramp_up, constant, ramp_down
        }
    };
    let sender_clone = sender.clone();
    let callback_receiver_clone = callback_receiver.clone();
    let callback_register_clone = callback_register.clone();
    let inital_streamers = vec![current];
    let mut mixer = Mixer::new(inital_streamers, vec![1]);
    let mixer_control_handle = mixer.control_handle();
    let mixer_callback_handle = mixer.get_callback_handle();
    if mixer_control_handle.add_gain_function(Arc::new(f)).is_err() {
        return;
    };
    let fade_config_clone = fade_config.clone();
    if mixer_callback_handle
        .add_callback(
            Duration::from_secs(fade_config_clone.cutoff_duration),
            Box::new(move || {
                loop_crossfade(
                    streamer_queue.clone(),
                    sender.clone(),
                    callback_receiver.clone(),
                    callback_register.clone(),
                    fade_config.clone(),
                    false,
                );
            }),
        )
        .is_err()
    {
        return;
    }
    let mixer_thread = mixer.play(
        sender_clone,
        callback_receiver_clone,
        callback_register_clone,
    );
    if let Err(some) = mixer_thread.join().unwrap() {
        println!("Error: {:?}", some);
    }
}

impl Streamer for PlayListStreamer {
    fn play(
        &mut self,
        sender: SyncSender<Vec<f32>>,
        callback_receiver: Receiver<Callback>,
        callback_register: SyncSender<u64>,
    ) -> JoinHandle<Result<(), StreamErr>> {
        let streamers = self.streamers.clone();
        if streamers.lock().unwrap().is_empty() {
            return thread::spawn(move || Ok(()));
        }

        let dur = streamers.lock().unwrap()[0].get_duration();
        let mut sample_cutoff = 0usize;
        let mut second_stream_cutoff = 0usize;
        let mut fade_in_function: Option<Box<dyn Fn(usize) -> f32 + Send + Sync>> = None;
        let mut fade_out_function: Option<Box<dyn Fn(usize) -> f32 + Send + Sync>> = None;
        let mut fade_out_after_s = 0u64;

        if let Some(duration) = dur
            && self.cross_fade_type != CrossFadeType::None
        {
            let duration_samples = self.get_input_channel_count() as usize
                * self.get_input_sample_rate() as usize
                * duration as usize;
            match self.cross_fade_type {
                CrossFadeType::Linear(fade_duration) => {
                    second_stream_cutoff = self.get_input_channel_count() as usize
                        * self.get_input_sample_rate() as usize
                        * fade_duration as usize;
                    sample_cutoff = duration_samples - second_stream_cutoff;
                    fade_in_function =
                        Some(Box::new(move |x| f_fadein_linear(x, second_stream_cutoff)));
                    fade_out_function =
                        Some(Box::new(move |x| f_fadeout_linear(x, second_stream_cutoff)));
                    fade_out_after_s = duration as u64 - fade_duration as u64;
                }
                CrossFadeType::Logarithmic(fade_duration) => {
                    second_stream_cutoff = self.get_input_channel_count() as usize
                        * self.get_input_sample_rate() as usize
                        * fade_duration as usize;
                    sample_cutoff = duration_samples - second_stream_cutoff;
                    fade_in_function =
                        Some(Box::new(move |x| f_fadein_log(x, second_stream_cutoff)));
                    fade_out_function =
                        Some(Box::new(move |x| f_fadeout_log(x, second_stream_cutoff)));
                    fade_out_after_s = duration as u64 - fade_duration as u64;
                }
                CrossFadeType::None => {}
            }
        }
        if sample_cutoff == 0 {
            thread::spawn(move || {
                loop_no_crossfade(
                    streamers.clone(),
                    sender.clone(),
                    callback_receiver,
                    callback_register,
                );
                Ok(())
            })
        } else {
            thread::spawn(move || {
                loop_crossfade(
                    streamers.clone(),
                    sender.clone(),
                    callback_receiver,
                    callback_register,
                    Arc::new(FadeConfig {
                        cutoff_duration: fade_out_after_s,
                        fade_in: fade_in_function.unwrap(),
                        fade_out: fade_out_function.unwrap(),
                    }),
                    true,
                );
                Ok(())
            })
        }
    }

    fn get_input_sample_rate(&self) -> u32 {
        self.streamers.lock().unwrap()[0].get_input_sample_rate()
    }

    fn get_input_channel_count(&self) -> u16 {
        self.streamers.lock().unwrap()[0].get_input_channel_count()
    }

    fn get_output_sample_rate(&self) -> u32 {
        self.streamers.lock().unwrap()[0].get_output_sample_rate()
    }

    fn finished_flag(&self) -> Arc<AtomicBool> {
        todo!()
    }

    fn get_callback_handle(&self) -> StreamerCallBackHandle {
        todo!()
    }

    fn control_handle(&self) -> ControlHandle {
        todo!()
    }

    fn get_duration(&self) -> Option<u64> {
        todo!()
    }
}
