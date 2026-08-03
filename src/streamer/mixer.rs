use crate::streamer::{ControlCommand, ControlHandle, DeviceOutputInfo, StreamErr, Streamer,
                      StreamerInputInfo, NO_SEEK,
};
use crossbeam_channel::{Sender, bounded, select};
use std::borrow::Cow;
use std::collections::VecDeque;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::thread::JoinHandle;
use crate::stream_player::StreamNotify;

enum MixerCommand {
    Stop(usize), // stop the nth channel
    Add {
        shared_buf: Arc<Mutex<VecDeque<f32>>>,
        index: Arc<AtomicUsize>,
        finished: Arc<AtomicBool>,
        weight: Arc<AtomicU32>,
        control: ControlHandle,
    },
}

fn apply_mixer_control(
    cmd: ControlCommand,
    children: &[ControlHandle],
    stopped: &Arc<AtomicBool>,
) -> bool {
    match cmd {
        ControlCommand::Stop => {
            stopped.store(true, Relaxed);
            for c in children {
                let _ = c.stop();
            }
            true
        }
        ControlCommand::Seek(time) => {
            for c in children {
               // let _ = c.seek(time);
            }
            false
        }
        ControlCommand::Rewind => {
            for c in children {
                let _ = c.rewind();
            }
            false
        }
        ControlCommand::AddGainFunction(gf) => {
            for c in children {
                let _ = c.add_gain_function(Arc::clone(&gf));
            }
            false
        }
        ControlCommand::RemoveGainFunction => {
            for c in children {
                let _ = c.remove_gain_function();
            }
            false
        }
    }
}

pub struct Mixer {
    streamers: Vec<Box<dyn Streamer>>,
    device_output_info: Option<DeviceOutputInfo>,
    weights: Vec<Arc<AtomicU32>>,
    finished: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    command_tx: Option<Sender<MixerCommand>>,
    // Retained after play() so add() can give new forwarder threads the ping channel.
    sync_sender: Option<Sender<usize>>,
    // Tracks samples mixed so far; new channels start their index here to avoid
    // stalling channels that are already ahead.
    play_position: Arc<AtomicUsize>,
    // Shared state so MixerHandle can access channels after Mixer is moved.
    shared: Arc<Mutex<MixerShared>>,
    control: ControlHandle,
    control_rx: Option<crossbeam_channel::Receiver<ControlCommand>>,
    // When true, output is averaged by 1/total_weight (safe for simultaneous
    // mixing). When false, channels are summed as-is — used for crossfades,
    // where the per-channel fade gains already sum to ~unity.
    normalize_gain: bool,
    last_seek_position: Arc<AtomicU64>,
}

struct MixerShared {
    command_tx: Option<Sender<MixerCommand>>,
    sync_sender: Option<Sender<usize>>,
    device_output_info: Option<DeviceOutputInfo>,
}

/// A handle to a `Mixer` that can be retained by the caller after the `Mixer`
/// is moved into a player. Allows adding/removing channels while playback is
/// running.
#[derive(Clone)]
pub struct MixerHandle {
    shared: Arc<Mutex<MixerShared>>,
    play_position: Arc<AtomicUsize>,
}

impl Mixer {
    pub fn new(streamers: Vec<Box<dyn Streamer>>, weights: Vec<u32>) -> Self {
        if weights.len() != streamers.len() {
            panic!("weights and streamers must have the same length");
        }
        let weights = weights
            .into_iter()
            .map(|x| Arc::new(AtomicU32::new(x)))
            .collect();
        let (control, control_rx) = ControlHandle::new();
        let device_output_info = None;
        Self {
            streamers,
            device_output_info,
            weights,
            finished: Arc::new(AtomicBool::new(false)),
            stopped: Arc::new(AtomicBool::new(false)),
            command_tx: None,
            sync_sender: None,
            control,
            control_rx: Some(control_rx),
            play_position: Arc::new(AtomicUsize::new(0)),
            shared: Arc::new(Mutex::new(MixerShared {
                command_tx: None,
                sync_sender: None,
                device_output_info,
            })),
            normalize_gain: true,
            last_seek_position: Arc::new(AtomicU64::new(NO_SEEK)),
        }
    }

    /// Disables (`false`) or enables (`true`) 1/total_weight averaging.
    /// Set to `false` for crossfade mixing so complementary fade gains sum
    /// to unity instead of being halved.
    pub fn set_normalize_gain(&mut self, normalize: bool) {
        self.normalize_gain = normalize;
    }

    /// Returns a `MixerHandle` that can be used to add/remove channels after
    /// the `Mixer` has been moved into a player and playback has started.
    pub fn handle(&self) -> MixerHandle {
        MixerHandle {
            shared: Arc::clone(&self.shared),
            play_position: Arc::clone(&self.play_position),
        }
    }

    pub fn add(&mut self, streamer: Box<dyn Streamer>, weight: u32, auto_seek: bool) {
        let weight_arc = Arc::new(AtomicU32::new(weight));

        if let Some(cmd_tx) = &self.command_tx && self.device_output_info.is_some() {
            Self::add_live(
                cmd_tx,
                self.sync_sender.as_ref().unwrap(),
                &self.play_position,
                streamer,
                Arc::clone(&weight_arc),
                auto_seek,
                self.device_output_info.clone().unwrap(),
            );
            self.weights.push(weight_arc);
        } else {
            self.streamers.push(streamer);
            self.weights.push(weight_arc);
        }
    }

    fn add_live(
        cmd_tx: &Sender<MixerCommand>,
        sync_sender: &Sender<usize>,
        play_position: &Arc<AtomicUsize>,
        mut streamer: Box<dyn Streamer>,
        weight_arc: Arc<AtomicU32>,
        auto_seek: bool,
        device_output_info: DeviceOutputInfo,
    ) {
        let current_pos = play_position.load(Relaxed);
        let shared_buf = Arc::new(Mutex::new(VecDeque::new()));
        let index = Arc::new(AtomicUsize::new(current_pos));
        let finished = streamer.finished_flag();
        let control = streamer.control_handle();

        let (inner_sender, inner_receiver) = mpsc::sync_channel::<Vec<f32>>(8);
       // streamer.play(device_output_info, inner_sender, stream_notifier.clone());
        if auto_seek {
            //TODO need to get the current position of the player first, and then seek
        }

        let sb = Arc::clone(&shared_buf);
        let ix = Arc::clone(&index);
        let ff = Arc::clone(&finished);
        let ss = sync_sender.clone();
        thread::spawn(move || {
            while let Ok(samples) = inner_receiver.recv()
                && !ff.load(Acquire)
            {
                sb.lock().unwrap().extend(samples.iter().copied());
                ix.fetch_add(samples.len(), AcqRel);
                ss.send(0).unwrap_or(());
            }
        });

        cmd_tx
            .send(MixerCommand::Add {
                shared_buf,
                index,
                finished,
                weight: weight_arc,
                control,
            })
            .unwrap_or(());
    }

    pub fn stop_channel(&self, ch_no: usize) {
        if let Some(tx) = &self.command_tx {
            tx.send(MixerCommand::Stop(ch_no)).unwrap_or(());
        }
    }

    pub fn get_streamers(&self) -> &Vec<Box<dyn Streamer>> {
        &self.streamers
    }

    pub fn get_weights(&self) -> Vec<u32> {
        self.weights.iter().map(|w| w.load(Relaxed)).collect()
    }

    pub fn set_weight(&mut self, index: usize, weight: u32) {
        self.weights[index].store(weight, Relaxed);
    }
}

impl MixerHandle {
    pub fn add(&self, streamer: Box<dyn Streamer>, weight: u32, auto_seek: bool) {
        let shared = self.shared.lock().unwrap();
        if let (Some(cmd_tx), Some(sync_sender), Some(device_output_info)) = (
            &shared.command_tx,
            &shared.sync_sender,
            &shared.device_output_info,
        ) {
            let weight_arc = Arc::new(AtomicU32::new(weight));
            Mixer::add_live(
                cmd_tx,
                sync_sender,
                &self.play_position,
                streamer,
                weight_arc,
                auto_seek,
                device_output_info.clone(),
            );
        }
    }

    pub fn stop_channel(&self, ch_no: usize) {
        let shared = self.shared.lock().unwrap();
        if let Some(tx) = &shared.command_tx {
            tx.send(MixerCommand::Stop(ch_no)).unwrap_or(());
        }
    }
}

impl Streamer for Mixer {
    fn play(
        &mut self,
        output_info: DeviceOutputInfo,
        sender: SyncSender<Vec<f32>>,
        stream_notifier: SyncSender<StreamNotify>,
    ) -> JoinHandle<Result<(), StreamErr>> {
        let (sync_sender, sync_receiver) = bounded::<usize>(8);
        let (cmd_tx, cmd_rx) = bounded::<MixerCommand>(4);
        self.command_tx = Some(cmd_tx);
        self.sync_sender = Some(sync_sender.clone());

        // Publish channels to the shared state so MixerHandle can use them.
        {
            let mut shared = self.shared.lock().unwrap();
            shared.command_tx = self.command_tx.clone();
            shared.sync_sender = self.sync_sender.clone();
        }

        // These Vecs are owned exclusively by the mixing thread. The Add command
        // arm appends to them directly — no locking required.
        let mut indices: Vec<Arc<AtomicUsize>> = Vec::new();
        let mut shared_bufs: Vec<Arc<Mutex<VecDeque<f32>>>> = Vec::new();
        let mut finished_flags: Vec<Arc<AtomicBool>> = Vec::new();
        let mut weights: Vec<Arc<AtomicU32>> = Vec::new();
        let mut children_control: Vec<ControlHandle> = Vec::new();

        for (s, w) in self.streamers.iter_mut().zip(self.weights.iter()) {
            let index = Arc::new(AtomicUsize::new(0));
            let shared_buf = Arc::new(Mutex::new(VecDeque::new()));
            let finished = s.finished_flag();
            let control = s.control_handle();

            let (inner_sender, inner_receiver) = mpsc::sync_channel::<Vec<f32>>(8);
            s.play(output_info, inner_sender, stream_notifier.clone());

            let ix = Arc::clone(&index);
            let sb = Arc::clone(&shared_buf);
            let ff = Arc::clone(&finished);
            let ss = sync_sender.clone();
            let j = indices.len();
            thread::spawn(move || {
                while let Ok(samples) = inner_receiver.recv()
                    && !ff.load(Acquire)
                {
                    sb.lock().unwrap().extend(samples.iter().copied());
                    ix.fetch_add(samples.len(), AcqRel);
                    ss.send(j).unwrap_or(());
                }
            });

            indices.push(index);
            shared_bufs.push(shared_buf);
            finished_flags.push(finished);
            weights.push(Arc::clone(w));
            children_control.push(control);
        }

        let finished = Arc::clone(&self.finished);
        let stopped = Arc::clone(&self.stopped);
        let paused = self.control.paused_flag();
        let control_rx = self.control_rx.take();
        let play_position = Arc::clone(&self.play_position);

        let normalize_gain = self.normalize_gain;

        thread::spawn(move || -> Result<(), StreamErr> {
            let control_rx = control_rx.ok_or(StreamErr::AlreadyPlaying)?;
            let mut prev_index = 0usize;

            'mix: loop {
                // Honor pause by parking the mixing thread. While parked we stop
                // draining `sync_receiver`, so backpressure propagates to the
                // children (their bounded inner channels fill and they block) —
                // no unbounded buffering. Transport control stays responsive.
                while paused.load(Acquire) {
                    if let Ok(cmd) = control_rx.try_recv() {
                        if apply_mixer_control(cmd, &children_control, &stopped) {
                            break 'mix;
                        }
                    }
                    thread::sleep(std::time::Duration::from_millis(10));
                }

                select! {
                    recv(sync_receiver) -> _ => {
                        if stopped.load(Acquire) {
                            break;
                        }

                        let len = indices.len();
                        let mut min_index = usize::MAX;
                        for j in 0..len {
                            if finished_flags[j].load(Acquire) {
                                continue;
                            }
                            min_index = min_index.min(indices[j].load(Acquire));
                        }

                        if min_index == usize::MAX {
                            break; // all channels finished
                        }

                        let v_size = min_index.saturating_sub(prev_index);
                        if v_size > 0 {
                            prev_index = min_index;
                            play_position.store(prev_index, Relaxed);

                            let total_weight: u32 = weights.iter()
                                .enumerate()
                                .filter(|(j, _)| !finished_flags[*j].load(Relaxed))
                                .map(|(_, w)| w.load(Relaxed))
                                .sum();
                            let koef = if !normalize_gain {
                                1.0
                            } else if total_weight > 0 {
                                1.0 / total_weight as f32
                            } else {
                                0.0
                            };

                            let mut output_data = vec![0.0f32; v_size];
                            for j in 0..len {
                                if finished_flags[j].load(Relaxed) {
                                    continue;
                                }
                                let mut buf = shared_bufs[j].lock().unwrap();
                                let available = buf.len().min(v_size);
                                if available == 0 {
                                    continue;
                                }
                                let weight = weights[j].load(Relaxed) as f32;
                                for (out, s) in output_data.iter_mut().zip(buf.drain(..available)) {
                                    *out += s * weight * koef;
                                }
                            }
                            sender.send(output_data).map_err(|_| StreamErr::SendError)?;
                        }
                    }
                    recv(cmd_rx) -> command => {
                        let Ok(cmd) = command else { continue; };
                        match cmd {
                            MixerCommand::Stop(ch_no) => {
                                if let Some(flag) = finished_flags.get(ch_no) {
                                    flag.store(true, Relaxed);
                                }
                            }
                            MixerCommand::Add { shared_buf, index, finished, weight, control } => {
                                shared_bufs.push(shared_buf);
                                indices.push(index);
                                finished_flags.push(finished);
                                weights.push(weight);
                                children_control.push(control);
                            }
                        }
                    }
                    recv(control_rx) -> command => {
                        if let Ok(cmd) = command {
                            if apply_mixer_control(cmd, &children_control, &stopped) {
                                break;
                            }
                        }
                    }
                }
            }

            finished.store(true, Release);
            Ok(())
        })
    }

    fn get_input_info(&self) -> Result<Cow<'_, StreamerInputInfo>, StreamErr> {
        todo!()
    }

    fn get_output_info(&self) -> Option<DeviceOutputInfo> {
        todo!()
    }

    fn finished_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.finished)
    }

    fn control_handle(&self) -> ControlHandle {
        self.control.clone()
    }

    fn last_seek_position(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.last_seek_position)
    }

    fn get_duration(&self) -> Option<u64> {
        let mut d: u64 = 0;
        for s in &self.streamers {
            if let Some(duration) = s.get_duration() {
                if duration < d || d == 0 {
                    d = duration;
                }
            }
        }
        if d == 0 { None } else { Some(d) }
    }
}
