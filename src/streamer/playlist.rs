use crate::streamer::mixer::{Mixer, MixerHandle};
use crate::streamer::utils::{f_fadein_linear, f_fadein_log, f_fadeout_linear, f_fadeout_log};
use crate::streamer::{
    ControlCommand, ControlHandle, DeviceOutputInfo, StreamErr, Streamer,
    StreamerInputInfo, add_callback,
};
use crossbeam_channel::Sender;
use std::borrow::Cow;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Debug, PartialEq, Clone, Copy)]
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
    command_rx: Option<crossbeam_channel::Receiver<ControlCommand>>,
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
            command_rx: None,
        }
    }
}

fn loop_no_crossfade(
    streamer_queue: Arc<Mutex<Vec<Box<dyn Streamer>>>>,
    sender: SyncSender<Vec<f32>>,
    device_output_info: DeviceOutputInfo,
) {
    let current = {
        let mut streamers = streamer_queue.lock().unwrap();
        if streamers.is_empty() {
            return;
        }
        streamers.remove(0)
    };

    let sender_clone = sender.clone();
    let inital_streamers = vec![current];
    let mut mixer = Mixer::new(inital_streamers, vec![1]);
    let mixer_thread = mixer.play(device_output_info, sender);
    if let Err(some) = mixer_thread.join().unwrap() {
        println!("Error: {:?}", some);
        return; // TODO work on the error type return from joinhandle
    }

    loop_no_crossfade(streamer_queue.clone(), sender_clone, device_output_info)
}

type FadeFn = Arc<dyn Fn(usize) -> f32 + Send + Sync>;

fn build_fades(fade_type: CrossFadeType, fade_samples: usize) -> (FadeFn, FadeFn) {
    match fade_type {
        CrossFadeType::Logarithmic(_) => (
            Arc::new(move |x| f_fadein_log(x, fade_samples)),
            Arc::new(move |x| f_fadeout_log(x, fade_samples)),
        ),
        _ => (
            Arc::new(move |x| f_fadein_linear(x, fade_samples)),
            Arc::new(move |x| f_fadeout_linear(x, fade_samples)),
        ),
    }
}

struct CrossfadeCtx {
    queue: Arc<Mutex<Vec<Box<dyn Streamer>>>>,
    handle: MixerHandle,
    fade_type: CrossFadeType,
    fade_secs: u64,
    fade_samples: usize,
}

fn schedule_crossfade(ctx: Arc<CrossfadeCtx>, outgoing_control: ControlHandle, cutoff_secs: u64) {
    let ctx_cb = ctx.clone();
    let cb: Box<dyn Fn() + Send> = Box::new(move || {
        let next = {
            let mut q = ctx_cb.queue.lock().unwrap();
            if q.is_empty() {
                return;
            }
            q.remove(0)
        };
        let next_dur = next.get_duration();
        let next_control = next.control_handle();
        let (fade_in, fade_out) = build_fades(ctx_cb.fade_type, ctx_cb.fade_samples);
        let _ = outgoing_control.add_gain_function(fade_out);
        let _ = next_control.add_gain_function(fade_in);
        ctx_cb.handle.add(next, 1, false);
        if let Some(d) = next_dur {
            let next_cutoff = cutoff_secs + d.saturating_sub(ctx_cb.fade_secs);
            schedule_crossfade(ctx_cb.clone(), next_control, next_cutoff);
        }
    });
    let _ = add_callback(Duration::from_secs(cutoff_secs), cb);
}

impl Streamer for PlayListStreamer {
    fn play(
        &mut self,
        output_info: DeviceOutputInfo,
        sender: SyncSender<Vec<f32>>,
    ) -> JoinHandle<Result<(), StreamErr>> {
        let streamers = self.streamers.clone();
        if streamers.lock().unwrap().is_empty() {
            return thread::spawn(move || Ok(()));
        }
        // Fade duration in whole seconds; 0 means "no crossfade".
        let fade_secs = match self.cross_fade_type {
            CrossFadeType::Linear(f) | CrossFadeType::Logarithmic(f) => f as u64,
            CrossFadeType::None => 0,
        };

        let first_dur = streamers.lock().unwrap()[0].get_duration();

        if fade_secs == 0 || first_dur.is_none() {
            return thread::spawn(move || {
                loop_no_crossfade(streamers.clone(), sender.clone(), output_info);
                Ok(())
            });
        }

        let first = streamers.lock().unwrap().remove(0);
        let fade_samples =
            output_info.channels as usize * output_info.sample_rate as usize * fade_secs as usize;
        let first_control = first.control_handle();

        let mut mixer = Mixer::new(vec![first], vec![1]);
        mixer.set_normalize_gain(false); // sum complementary fade gains, don't average
        let handle = mixer.handle();

        let ctx = Arc::new(CrossfadeCtx {
            queue: streamers.clone(),
            handle: handle.clone(),
            fade_type: self.cross_fade_type,
            fade_secs,
            fade_samples,
        });

        let cutoff = first_dur.unwrap().saturating_sub(fade_secs);
        schedule_crossfade(ctx, first_control, cutoff);

        mixer.play(output_info, sender)
    }

    fn get_input_info(&self) -> Result<Cow<'_, StreamerInputInfo>, StreamErr> {
        todo!()
    }

    fn get_output_info(&self) -> Option<DeviceOutputInfo> {
        todo!()
    }

    fn finished_flag(&self) -> Arc<AtomicBool> {
        todo!()
    }

    fn control_handle(&self) -> ControlHandle {
        todo!()
    }

    fn get_duration(&self) -> Option<u64> {
        todo!()
    }
}
