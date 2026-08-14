use crate::stream_player::{PlayerStatus, StreamNotify};
use crate::streamer::mixer::{Mixer, MixerHandle};
use crate::streamer::utils::{f_fadein_linear, f_fadein_log, f_fadeout_linear, f_fadeout_log};
use crate::streamer::{
    add_callback, ControlCommand, ControlHandle, DeviceOutputInfo, StreamErr, Streamer,
    StreamerInputInfo, NO_SEEK,
};
use crossbeam_channel::Sender;
use std::borrow::Cow;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum CrossFadeType {
    Logarithmic(f32),
    Linear(f32),
    None,
}

const NO_POS: u16 = u16::MAX;

pub struct PlayListStreamer {
    streamers: Arc<Mutex<Vec<Box<dyn Streamer>>>>, //looping in child thread and removing elements from vec
    control: ControlHandle,
    sync_tx: Option<Sender<usize>>,
    command_rx: Option<crossbeam_channel::Receiver<ControlCommand>>,
    cross_fade_type: CrossFadeType,
    last_seek_position: Arc<AtomicU64>,
    current_list_index: Arc<AtomicU16>,
}

impl PlayListStreamer {
    pub fn new(streamers: Vec<Box<dyn Streamer>>, cross_fade_type: CrossFadeType) -> Self {
        let (control, control_rx) = ControlHandle::new();
        Self {
            streamers: Arc::new(Mutex::new(streamers)),
            cross_fade_type,
            control,
            command_rx: Some(control_rx),
            sync_tx: None,
            last_seek_position: Arc::new(AtomicU64::new(NO_SEEK)),
            current_list_index: Arc::new(AtomicU16::new(NO_POS)),
        }
    }
}

fn loop_no_crossfade(
    streamer_queue: Arc<Mutex<Vec<Box<dyn Streamer>>>>,
    sender: SyncSender<Vec<f32>>,
    player_status: PlayerStatus,
    stream_notifier: SyncSender<StreamNotify>,
    current_handle: Arc<Mutex<Option<ControlHandle>>>, //instead of creating another listner thread, we operate on underlying streamer from the main command thread in play
) {
    let ch_new = Arc::clone(&current_handle);
    let mut current = {
        let mut streamers = streamer_queue.lock().unwrap();
        if streamers.is_empty() {
            return;
        }
        let curr = streamers.remove(0);
        let new_control_handler = curr.control_handle();
        let mut guard = current_handle.lock().unwrap();
        *guard = Some(new_control_handler);
        curr
    };

    // TODO its moved here, before the move do the clone (re-spawn), and push it to vec
    let sender_clone = sender.clone();
    let ps_clone = player_status.clone();
    let handle = current.play(player_status, sender, stream_notifier.clone());
    if let Err(some) = handle.join().unwrap() {
        println!("Error: {:?}", some);
        return; // TODO work on the error type return from joinhandle
    }

    loop_no_crossfade(
        Arc::clone(&streamer_queue),
        sender_clone,
        ps_clone,
        stream_notifier.clone(),
        ch_new,
    )
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
    respawned: Arc<Mutex<Vec<Box<dyn Streamer>>>>, //in order to play the stream it has to be moved, before that we "respawn" it, and save here in case rewind happens
    handle: MixerHandle,
    fade_type: CrossFadeType,
    fade_secs: u64,
    fade_samples: usize,
}

fn schedule_crossfade(
    ctx: Arc<CrossfadeCtx>,
    outgoing_control: ControlHandle,
    cutoff_secs: u64,
    current_pos: Arc<AtomicU16>,
) {
    let ctx_cb = Arc::clone(&ctx);

    let curr_poss = Arc::clone(&current_pos);
    let cb: Box<dyn Fn() + Send> = Box::new(move || {
        curr_poss.fetch_add(1, Ordering::Relaxed);
        let next = {
            let mut q = ctx_cb.queue.lock().unwrap();
            if q.is_empty() {
                return;
            }
            q.remove(0)
        };
        let respawned = next.respawn();
        // we have to clone ("re-spawn") in advance, because handle.add consumes next
        if respawned.is_ok() {
            ctx_cb.respawned.lock().unwrap().push(respawned.unwrap())
        }
        let next_dur = next.get_duration();
        let next_control = next.control_handle();
        let (fade_in, fade_out) = build_fades(ctx_cb.fade_type, ctx_cb.fade_samples);
        let _ = outgoing_control.add_gain_function(fade_out);
        let _ = next_control.add_gain_function(fade_in);
        ctx_cb.handle.add(next, 1, false);
        if let Some(d) = next_dur {
            let next_cutoff = cutoff_secs + d.saturating_sub(ctx_cb.fade_secs);
            schedule_crossfade(
                Arc::clone(&ctx_cb),
                next_control,
                next_cutoff,
                Arc::clone(&current_pos),
            );
        }
    });
    let _ = add_callback(Duration::from_secs(cutoff_secs), cb);
}

impl Streamer for PlayListStreamer {
    fn play(
        &mut self,
        // output_info: DeviceOutputInfo,
        player_status: PlayerStatus,
        sender: SyncSender<Vec<f32>>,
        stream_notifier: SyncSender<StreamNotify>,
    ) -> JoinHandle<Result<(), StreamErr>> {
        let start_media_samples_elapsed =
            player_status.media_elapsed_samples.load(Ordering::Relaxed);
        // it can happen that we start the playlist after something else was played in stream player
        // and we need relative times to skip the playlist items
        let output_info = player_status.device_output_info;
        let streamers = Arc::clone(&self.streamers);
        if streamers.lock().unwrap().is_empty() {
            return thread::spawn(move || Ok(()));
        }
        // Fade duration in whole seconds; 0 means "no crossfade".
        let fade_secs = match self.cross_fade_type {
            CrossFadeType::Linear(f) | CrossFadeType::Logarithmic(f) => f as u64,
            CrossFadeType::None => 0,
        };

        let first_dur = streamers.lock().unwrap()[0].get_duration();

        let command_rx = self.command_rx.clone();
        let paused = self.control.paused_flag();
        let current_handle: Arc<Mutex<Option<ControlHandle>>> = Arc::new(Mutex::new(None));
        let ch_clone = Arc::clone(&current_handle);
        let current_track = Arc::new(AtomicU16::new(0));
        /*
        TODO

        Appart form current track(and maybe that is not required)
        we need to track the finished time of tracks, so we know where to seek to
        the question is wether to keep the time or samples(so its atomic vs mutex)
        */
        thread::spawn(move || -> Result<(), StreamErr> {
            let cmd_rx = command_rx.ok_or(StreamErr::AlreadyPlaying)?;
            loop {
                while paused.load(Ordering::Acquire) {
                    thread::sleep(Duration::from_millis(10));
                }
                let received = cmd_rx.recv();
                let ch = current_handle.lock().unwrap();
                match received {
                    Ok(ControlCommand::Seek(time)) => {
                        //TODO after a streaming calculations for a playback, send underlying seek
                        // to a current_handle if there is one, and send there
                        // calcuate the time based on already passed time
                    }
                    Ok(ControlCommand::Stop) => {
                        if ch.is_some() {
                            ch.as_ref().unwrap().stop()?;
                        }
                    }
                    Ok(ControlCommand::Rewind) => {}
                    Ok(ControlCommand::AddGainFunction(gf)) => {
                        if ch.is_some() {
                            ch.as_ref().unwrap().add_gain_function(gf)?;
                        }
                    }
                    Ok(ControlCommand::RemoveGainFunction) => {
                        if ch.is_some() {
                            ch.as_ref().unwrap().remove_gain_function()?;
                        }
                    }
                    Err(_) => {}
                }
            }
        });

        if fade_secs == 0 || first_dur.is_none() {
            return thread::spawn(move || -> _ {
                loop_no_crossfade(
                    Arc::clone(&streamers),
                    sender.clone(),
                    player_status.clone(),
                    stream_notifier.clone(),
                    ch_clone,
                );
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
        let mut chl = ch_clone.lock().unwrap();
        *chl = Some(mixer.control_handle());

        let ctx = Arc::new(CrossfadeCtx {
            queue: Arc::clone(&streamers),
            respawned: Arc::new(Mutex::new(vec![])),
            handle: handle.clone(),
            fade_type: self.cross_fade_type,
            fade_secs,
            fade_samples,
        });

        let cutoff = first_dur.unwrap().saturating_sub(fade_secs);

        schedule_crossfade(ctx, first_control, cutoff,Arc::clone(&current_track));

        mixer.play(player_status, sender, stream_notifier.clone())
    }

    fn get_input_info(&self) -> Result<Cow<'_, StreamerInputInfo>, StreamErr> {
        todo!()
    }

    fn get_output_info(&self) -> Option<DeviceOutputInfo> {
        todo!()
    }

    fn finished_flag(&self) -> Arc<AtomicBool> {
        self.control.finished_flag()
    }

    // TODO detect when playlist is finished, and set finished flag
    fn control_handle(&self) -> ControlHandle {
        self.control.clone()
    }

    fn last_seek_position(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.last_seek_position)
    }

    fn get_duration(&self) -> Option<u64> {
        todo!()
    }

    fn respawn(&self) -> Result<Box<dyn Streamer>, StreamErr> {
        let respawned = self
            .streamers
            .lock()
            .unwrap()
            .iter()
            .map(|s| s.respawn())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Box::new(PlayListStreamer::new(
            respawned,
            self.cross_fade_type,
        )))
    }
}
