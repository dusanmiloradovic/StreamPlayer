use crate::stream_player::{PlayerStatus, StreamNotify};
use crate::streamer::mixer::{Mixer, MixerHandle};
use crate::streamer::utils::{f_fadein_linear, f_fadein_log, f_fadeout_linear, f_fadeout_log};
use crate::streamer::{
    ControlCommand, ControlHandle, DeviceOutputInfo, NO_SEEK, StreamErr, Streamer,
    StreamerInputInfo, add_callback,
};
use crossbeam_channel::Sender;
use std::borrow::Cow;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use crate::stream_player::BitRateInfo::Streamer;

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
    played_so_far: Arc<Mutex<f64>>,
    passed_streams: Arc<Mutex<Vec<Box<dyn Streamer>>>>,
    stopped_loop: Arc<AtomicBool>,
) {
    let ch_new = Arc::clone(&current_handle);
    if stopped_loop.load(Ordering::Relaxed) {
        return;
    }
    let mut current = {
        let mut streamers = streamer_queue.lock().unwrap();
        if streamers.is_empty() {
            return;
        }
        let curr = streamers.remove(0);
        let new_control_handler = curr.control_handle();
        let mut guard = current_handle.lock().unwrap();
        *guard = Some(new_control_handler);
        let respawned = curr.respawn();
        if let Ok(respawned) = respawned {
            passed_streams.lock().unwrap().push(respawned);
        }
        curr
    };

    // TODO its moved here, before the move do the clone (re-spawn), and push it to vec
    let sender_clone = sender.clone();
    let ps_clone = player_status.clone();
    let dur = current.get_duration();
    let handle = current.play(player_status, sender, stream_notifier.clone());
    if let Err(some) = handle.join().unwrap() {
        println!("Error: {:?}", some);
        return; // TODO work on the error type return from joinhandle
    }
    if let Some(dur) = dur {
        let mut psf_lock = played_so_far.lock().unwrap();
        *psf_lock += dur;
    }

    loop_no_crossfade(
        Arc::clone(&streamer_queue),
        sender_clone,
        ps_clone,
        stream_notifier.clone(),
        ch_new,
        Arc::clone(&played_so_far),
        Arc::clone(&passed_streams),
        Arc::clone(&stopped_loop),
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

// TODO we need to store the last callback time in shared structure
// and we need to call re_schedule in case of seek
// (maybe not if its still the same stream and its not yet passed the time for fade out)
struct CrossfadeCtx {
    queue: Arc<Mutex<Vec<Box<dyn Streamer>>>>,
    respawned: Arc<Mutex<Vec<Box<dyn Streamer>>>>, //in order to play the stream it has to be moved, before that we "respawn" it, and save here in case rewind happens
    handle: MixerHandle,
    fade_type: CrossFadeType,
    fade_secs: f64,
    fade_samples: usize,
    next_run: Arc<Mutex<Duration>>, //when there is a seek, we need first to stop the callbacks, we will use this
}

fn schedule_crossfade(
    ctx: Arc<CrossfadeCtx>,
    outgoing_control: ControlHandle,
    cutoff_secs: f64,
    current_pos: Arc<AtomicU16>,
) {
    let ctx_cb = Arc::clone(&ctx);
    let nr = Arc::clone(&ctx.next_run);

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
        if let Ok(respawned) = respawned {
            ctx_cb.respawned.lock().unwrap().push(respawned);
        }
        let next_dur = next.get_duration();
        let next_control = next.control_handle();
        let (fade_in, fade_out) = build_fades(ctx_cb.fade_type, ctx_cb.fade_samples);
        let _ = outgoing_control.add_gain_function(fade_out);
        let _ = next_control.add_gain_function(fade_in);
        ctx_cb.handle.add(next, 1, false);
        if let Some(d) = next_dur {
            let next_cutoff = cutoff_secs + (d - ctx_cb.fade_secs).max(0.0);
            schedule_crossfade(
                Arc::clone(&ctx_cb),
                next_control,
                next_cutoff,
                Arc::clone(&current_pos),
            );
        }
    });
    let dura = Duration::from_secs_f64(cutoff_secs.max(0.0));
    let _ = add_callback(dura, cb, false);
    *nr.lock().unwrap() = dura;
}

impl Streamer for PlayListStreamer {
    fn play(
        &mut self,
        // output_info: DeviceOutputInfo,
        player_status: PlayerStatus,
        sender: SyncSender<Vec<f32>>,
        stream_notifier: SyncSender<StreamNotify>,
    ) -> JoinHandle<Result<(), StreamErr>> {
        let output_info = player_status.device_output_info;
        let streamers = Arc::clone(&self.streamers);
        if streamers.lock().unwrap().is_empty() {
            return thread::spawn(move || Ok(()));
        }
        // Fade duration in seconds; 0 means "no crossfade".
        let fade_secs = match self.cross_fade_type {
            CrossFadeType::Linear(f) | CrossFadeType::Logarithmic(f) => f as f64,
            CrossFadeType::None => 0.0,
        };

        let first_dur = streamers.lock().unwrap()[0].get_duration();

        let command_rx = self.command_rx.clone();
        let paused = self.control.paused_flag();
        let current_handle: Arc<Mutex<Option<ControlHandle>>> = Arc::new(Mutex::new(None));
        let ch_clone = Arc::clone(&current_handle);
        let current_track = Arc::new(AtomicU16::new(0));
        let played_finished_so_far = Arc::new(Mutex::new(0f64));
        let respawned_streamers: Arc<Mutex<Vec<Box<dyn Streamer>>>> = Arc::new(Mutex::new(vec![]));
        let mut mixer_handle: Option<MixerHandle> = None;
        let stopped_no_crossfade_loop: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
        // this is used to mark no_crossfade loop that it shouldn't continue
        // that will happen on Stop, Seek and Rewind
        /*
        we need to track the finished time of tracks, so we know where to seek to
        in the child track
         */
        let ch_seek_clone = Arc::clone(&current_handle);
        let respawned_streamers_clone = Arc::clone(&respawned_streamers);
        thread::spawn(move || -> Result<(), StreamErr> {
            let cmd_rx = command_rx.ok_or(StreamErr::AlreadyPlaying)?;
            loop {
                while paused.load(Ordering::Acquire) {
                    thread::sleep(Duration::from_millis(10));
                }
                let received = cmd_rx.recv();
                let ch = current_handle.lock().unwrap();
                let strmrs =  Arc::clone(&streamers);
                match received {
                    Ok(ControlCommand::Seek(time)) => {
                        //TODO after a streaming calculations for a playback, send underlying seek
                        // to a current_handle if there is one, and send there
                        // calcuate the time based on already passed time

                        if let Some(ch) = ch_seek_clone.lock().unwrap().as_ref() {
                           // ch.stop()?;
                            // first this will be done only for no crossfade
                            if mixer_handle.is_none() {
                                stopped_no_crossfade_loop.store(true, Ordering::Relaxed);
                                let psf = *played_finished_so_far.lock().unwrap();
                                let mut time = time - psf;
                                let mut sought_streamer = 0usize;
                                let mut prev_time=time;
                                if time < 0f64 {
                                    // TODO backward search
                                } else {
                                    while time>0f64{
                                        prev_time=time;
                                        let dur={
                                            let lck = strmrs.lock().unwrap();
                                            let curr = lck.get(sought_streamer);
                                            if curr.is_none(){
                                                // shouldn't happen, but just in case
                                                println!("no streamer for {}", sought_streamer);
                                                sought_streamer +=1;
                                                break;
                                            }
                                            else{
                                                curr.unwrap().get_duration()
                                            }
                                        };
                                        if dur.is_none(){
                                            return Err(StreamErr::SeekNotSupported);
                                        }
                                        time -= dur.unwrap();
                                    }
                                    time = prev_time;
                                    ch.stop()?;
                                    // TODO don't do this if the index is 0, meaning seek is in the current handle
                                    // right now even for this its stopping and moving back
                                    let mut r_q = respawned_streamers_clone.lock().unwrap();
                                    let mut s_q = strmrs.lock().unwrap();
                                    let mut rl =r_q.len();
                                    while rl>=sought_streamer{
                                        let v = r_q.pop();
                                        if v.is_none(){
                                            break;
                                        }
                                        s_q.insert(0, v.unwrap());
                                        rl-=1;
                                    }

                                     //TODO adjust played so far (psf below)
                                    loop_no_crossfade(
                                        Arc::clone(&streamers),
                                        sender.clone(),
                                        player_status.clone(),
                                        stream_notifier.clone(),
                                        ch_clone,
                                        psf,
                                        Arc::clone(&respawned_streamers),
                                        Arc::clone(&stopped_no_crossfade_loop),
                                    )
                                }
                            } else {
                                // TODO do for
                            }
                        }
                    }
                    Ok(ControlCommand::Stop) => {
                        if ch.is_some() {
                            ch.as_ref().unwrap().stop()?;
                            stopped_no_crossfade_loop.store(true, Ordering::Relaxed);
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

        if fade_secs <= 0.0 || first_dur.is_none() {
            let psf = Arc::clone(&played_finished_so_far);
            return thread::spawn(move || -> _ {
                loop_no_crossfade(
                    Arc::clone(&streamers),
                    sender.clone(),
                    player_status.clone(),
                    stream_notifier.clone(),
                    ch_clone,
                    psf,
                    Arc::clone(&respawned_streamers),
                    Arc::clone(&stopped_no_crossfade_loop),
                );
                Ok(())
            });
        }

        let first = streamers.lock().unwrap().remove(0);
        let fade_samples =
            (output_info.channels as f64 * output_info.sample_rate as f64 * fade_secs) as usize;
        let first_control = first.control_handle();

        let mut mixer = Mixer::new(vec![first], vec![1]);
        mixer.set_normalize_gain(false); // sum complementary fade gains, don't average

        let handle = mixer.handle();
        let mut chl = ch_clone.lock().unwrap();
        *chl = Some(mixer.control_handle());
        mixer_handle = Some(handle.clone());
        let rs = mixer.respawn();
        if let Ok(rs) = rs {
            respawned_streamers.lock().unwrap().push(rs);
            // This should be at pos 0, TODO do we need to ensure that?
        }

        let ctx = Arc::new(CrossfadeCtx {
            queue: Arc::clone(&streamers),
            respawned: Arc::clone(&respawned_streamers),
            handle: handle.clone(),
            fade_type: self.cross_fade_type,
            fade_secs,
            fade_samples,
            next_run: todo!(),
        });

        let cutoff = (first_dur.unwrap() - fade_secs).max(0.0);

        schedule_crossfade(ctx, first_control, cutoff, Arc::clone(&current_track));

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

    fn get_duration(&self) -> Option<f64> {
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
