use crate::streamer::mixer::Mixer;
use crate::streamer::utils::{f_fadein_linear, f_fadein_log, f_fadeout_linear, f_fadeout_log};
use crate::streamer::{
    Callback, ControlCommand, ControlHandle, StreamErr, Streamer, StreamerCallBackHandle,
    StreamerCallbackShared,
};
use async_broadcast::Receiver;
use crossbeam_channel::Sender;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

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
    callback_handle: Arc<Mutex<StreamerCallbackShared>>,
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
            callback_handle: Arc::new(Mutex::new(StreamerCallbackShared::new())),
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
    let streamers = streamer_queue.lock().unwrap();
    if streamers.len() == 0 {
        return ();
    }
    let sender_clone = sender.clone();
    let callback_receiver_clone = callback_receiver.clone();
    let callback_register_clone = callback_register.clone();
    let current = streamer_queue.lock().unwrap().remove(0);
    let inital_streamers = vec![current];
    let mut mixer = Mixer::new(inital_streamers, vec![1]);
    let mixer_thread = mixer.play(sender, callback_receiver, callback_register);
    if let Err(some) = mixer_thread.join().unwrap() {
        println!("Error: {:?}", some);
        return (); // TODO work on the error type return from joinhandle
    }
    loop_no_crossfade(
        streamer_queue.clone(),
        sender_clone,
        callback_receiver_clone,
        callback_register_clone,
    )
}

impl Streamer for PlayListStreamer {
    fn play(
        &mut self,
        sender: SyncSender<Vec<f32>>,
        callback_receiver: Receiver<Callback>,
        callback_register: SyncSender<u64>,
    ) -> JoinHandle<Result<(), StreamErr>> {
        let streamers = self.streamers.clone();
        if streamers.lock().unwrap().len() == 0 {
            return thread::spawn(move || Ok(()));
        }

        let current = streamers.lock().unwrap().remove(0);
        let dur = current.get_duration();
        let mut sample_cutoff = 0 as usize;
        let mut second_stream_cutoff = 0 as usize;
        let mut fade_in_function: Option<Box<dyn Fn(usize) -> f32>> = None;
        let mut fade_out_function: Option<Box<dyn Fn(usize) -> f32>> = None;
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
                    fade_in_function = Some(Box::new(|x| f_fadein_linear(x, second_stream_cutoff)));
                    fade_out_function =
                        Some(Box::new(|x| f_fadeout_linear(x, second_stream_cutoff)));
                }
                CrossFadeType::Logarithmic(fade_duration) => {
                    second_stream_cutoff = self.get_input_channel_count() as usize
                        * self.get_input_sample_rate() as usize
                        * fade_duration as usize;
                    sample_cutoff = duration_samples - second_stream_cutoff;
                    fade_in_function = Some(Box::new(|x| f_fadein_log(x, second_stream_cutoff)));
                    fade_out_function = Some(Box::new(|x| f_fadeout_log(x, second_stream_cutoff)));
                }
                CrossFadeType::None => {}
            }
        }
        if sample_cutoff == 0 {
            return thread::spawn(move || {
                loop_no_crossfade(
                    streamers.clone(),
                    sender.clone(),
                    callback_receiver,
                    callback_register,
                );
                Ok(())
            });
        }
        let initial_streamers = vec![current];
        let mut mixer = Mixer::new(initial_streamers, vec![1]);
        let mixer_handle = mixer.handle();
        let callback_receiver_clone = callback_receiver.clone();
        let callback_register_clone = callback_register.clone();
        let new_sender = sender.clone();
        mixer.play(sender, callback_receiver, callback_register)

    }

    fn get_input_sample_rate(&self) -> u32 {
        self.streamers[0].get_input_sample_rate()
    }

    fn get_input_channel_count(&self) -> u16 {
        self.streamers[0].get_input_channel_count()
    }

    fn get_output_sample_rate(&self) -> u32 {
        self.streamers[0].get_output_sample_rate()
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
