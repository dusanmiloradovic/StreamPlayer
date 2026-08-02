use crossbeam_channel::{Sender, bounded};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::atomic::Ordering::{Acquire, Release};
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::{Arc, LazyLock, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use symphonia::core::codecs::CodecParameters;

pub mod mixer;
pub mod playlist;
pub mod single;
pub mod utils;

#[derive(Debug)]
pub enum StreamErr {
    NoSampleRate,
    UnsupportedFormat,
    NoAudioTrack,
    NoOutputDevice,
    UnsupportedChannelCount,
    UnsupportedCodec,
    UnknownError,
    QueryOutputDeviceError,
    NoDeviceConfigForChannelCount,
    ResamplingError,
    OutputStreamError,
    SendError,
    AlreadyPlaying,
    NotPlaying,
    InputInfoError,
    ProbeError,
    SeekError,
}

pub struct StreamerCallbackShared {
    callback_register: Mutex<Option<SyncSender<u64>>>,
    pending_callbacks: Mutex<HashMap<Duration, Box<dyn Fn() + Send>>>,
    callbacks: Mutex<HashMap<u64, Box<dyn Fn() + Send>>>, //once the streamer starts playing it sends the sample rate, so duration can be calculated
    device_output_info: Mutex<Option<DeviceOutputInfo>>, // comes from device, used to convert Duration to number of samples
}

#[derive(Debug)]
pub enum StreamerAddError {
    NoSampleRate,
}

impl Default for StreamerCallbackShared {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamerCallbackShared {
    fn convert_duration_to_samples(&self, d: Duration) -> u64 {
        let do_info = self.device_output_info.lock().unwrap();
        if do_info.is_none() {
            return 0;
        }
        let device_output_info = do_info.unwrap();
        (d.as_secs_f64() * device_output_info.sample_rate as f64 * device_output_info.channels as f64) as u64
    }
    pub fn new() -> Self {
        Self {
            pending_callbacks: Mutex::new(HashMap::new()),
            callbacks: Mutex::new(HashMap::new()),
            device_output_info: Mutex::new(None),
            callback_register: Mutex::new(None),
        }
    }
    pub fn add_callback(
        &self,
        after: Duration,
        callback: Box<dyn Fn() + Send>,
    ) -> Result<(), StreamerAddError> {
        let cbr = self.callback_register.lock().unwrap();
        match &*cbr {
            None => {
                let mut pending_callbacks = self.pending_callbacks.lock().unwrap();
                pending_callbacks.insert(after, callback);
            }
            Some(cr) => {
                let mut callbacks = self.callbacks.lock().unwrap();
                let samples_duration = self.convert_duration_to_samples(after);
                callbacks.insert(samples_duration, callback);
                // when there is callback register, there is output info as well (set at streamer), so we can calculate no of samples
                cr.send(samples_duration).unwrap(); //TODO handle this
            }
        }
        Ok(())
    }
    pub fn set_callback_receiver(
        &self,
        mut cr: Receiver<Callback>,
        callback_register: SyncSender<u64>,
        device_output_info: DeviceOutputInfo,
    ) {
        *self.device_output_info.lock().unwrap() = Some(device_output_info);
        {
            let mut pcl = self.pending_callbacks.lock().unwrap();
            let mut callbacks = self.callbacks.lock().unwrap();
            for (key, val) in pcl.drain() {
                let samples_duration = self.convert_duration_to_samples(key);
                callbacks.insert(samples_duration, val);
                callback_register.send(samples_duration).unwrap();
            }
        }
        *self.callback_register.lock().unwrap() = Some(callback_register);
        thread::spawn(move || {
            // captures only `cr`; references SHARED directly
            while let cbr = cr.recv() {
                match cbr {
                    Ok(Callback::CbOnSample(cb_time)) => {
                        let cb = CALLBACK_SHARED.callbacks.lock().unwrap().remove(&cb_time);
                        if let Some(cb) = cb {
                            cb();
                        }
                    }
                    Err(_) => {} //TODO analyze when this can happen and how to handle error
                }
            }
        });
    }
}

static CALLBACK_SHARED: LazyLock<StreamerCallbackShared> =
    LazyLock::new(|| StreamerCallbackShared::new());

pub(crate) fn callback_shared() -> &'static StreamerCallbackShared {
    &CALLBACK_SHARED
}

pub fn add_callback(
    after: Duration,
    callback: Box<dyn Fn() + Send>,
) -> Result<(), StreamerAddError> {
    CALLBACK_SHARED.add_callback(after, callback)
}

pub enum ControlCommand {
    Stop,
    Seek(u64),
    Rewind,
    AddGainFunction(Arc<dyn Fn(usize) -> f32 + Send + Sync>),
    RemoveGainFunction,
}

#[derive(Clone)]
pub struct StreamerInputInfo {
    track_id: u32,
    pub(crate) channels: u16,
    pub(crate) sample_rate: u32,
    duration: Option<u64>,
    codec_params: CodecParameters,
}

#[derive(Debug, Clone, Copy)]
pub struct DeviceOutputInfo {
    pub channels: u16,
    pub sample_rate: u32,
}

#[derive(Clone)]
pub struct ControlHandle {
    paused: Arc<AtomicBool>,
    command_tx: Sender<ControlCommand>,
}

impl ControlHandle {
    /// Creates a handle together with the receiver that the owning streamer's
    /// `play()` loop must drain.
    pub fn new() -> (ControlHandle, crossbeam_channel::Receiver<ControlCommand>) {
        let (command_tx, command_rx) = bounded(4);
        (
            ControlHandle {
                paused: Arc::new(AtomicBool::new(false)),
                command_tx,
            },
            command_rx,
        )
    }

    /// The shared pause flag, honored by the owning streamer's play loop.
    pub fn paused_flag(&self) -> Arc<AtomicBool> {
        self.paused.clone()
    }

    pub fn pause(&self) {
        self.paused.store(true, Release);
    }

    pub fn resume(&self) {
        self.paused.store(false, Release);
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Acquire)
    }

    pub fn stop(&self) -> Result<(), StreamErr> {
        self.send(ControlCommand::Stop)
    }

    pub fn seek(&self, time: u64) -> Result<(), StreamErr> {
        self.send(ControlCommand::Seek(time))
    }

    pub fn rewind(&self) -> Result<(), StreamErr> {
        self.send(ControlCommand::Rewind)
    }

    fn send(&self, command: ControlCommand) -> Result<(), StreamErr> {
        self.command_tx
            .send(command)
            .map_err(|_| StreamErr::SendError)
    }

    pub fn add_gain_function(
        &self,
        function: Arc<dyn Fn(usize) -> f32 + Send + Sync>,
    ) -> Result<(), StreamErr> {
        // Arc instead of Box since we can reuse the function between child streams
        self.send(ControlCommand::AddGainFunction(function))
    }

    pub fn remove_gain_function(&self) -> Result<(), StreamErr> {
        self.send(ControlCommand::RemoveGainFunction)
    }
}

pub trait Streamer: Send {
    fn play(
        &mut self,
        output_info: DeviceOutputInfo,
        sender: SyncSender<Vec<f32>>,
    ) -> JoinHandle<Result<(), StreamErr>>;
    fn get_input_info(&self) -> Result<Cow<'_, StreamerInputInfo>, StreamErr>;
    // TODO *decoded.spec.rate holds always the decoded sample rate, maybe use that as alternative
    fn get_output_info(&self) -> Option<DeviceOutputInfo>;
    fn finished_flag(&self) -> Arc<AtomicBool>;
    /// Cloneable transport-control surface (stop/pause/resume/seek/rewind),
    /// safe to capture in a sample callback.
    fn control_handle(&self) -> ControlHandle;
    fn get_duration(&self) -> Option<u64>;
    fn last_seek_position(&self) -> Arc<Option<AtomicU64>>;
}

#[derive(Clone)]
pub enum Callback {
    CbOnSample(u64),
}
