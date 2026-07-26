use async_broadcast::Receiver;
use crossbeam_channel::{Sender, bounded};
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Acquire, Release};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use std::borrow::Cow;
use symphonia::core::codecs::CodecParameters;
use symphonia::core::probe::ProbeResult;

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
}

pub struct StreamerCallbackShared {
    callback_register: Option<SyncSender<Duration>>,
    pending_callbacks: Arc<Mutex<Vec<Duration>>>,
    callbacks: Arc<Mutex<HashMap<Duration, Box<dyn Fn() + Send>>>>,
}

#[derive(Debug)]
pub enum StreamerAddError {
    NoSampleRate,
}

pub struct StreamerCallBackHandle {
    shared: Arc<Mutex<StreamerCallbackShared>>,
}

//TODO move all the callback logic here, including with the receiving
impl StreamerCallbackShared {
    pub fn add_callback(
        &self,
        after: Duration,
        callback: Box<dyn Fn() + Send>,
    ) -> Result<(), StreamerAddError> {
        let mut pending_callbacks = self.pending_callbacks.lock().unwrap();
        let mut callbacks = self.callbacks.lock().unwrap();
        callbacks.insert(after, callback);
        match &self.callback_register {
            None => {
                pending_callbacks.push(after);
            }
            Some(cr) => {
                cr.send(after).unwrap();
            }
        }
        Ok(())
    }
}

impl StreamerCallBackHandle {
    pub fn add_callback(
        &self,
        after: Duration,
        callback: Box<dyn Fn() + Send>,
    ) -> Result<(), StreamerAddError> {
        self.shared.lock().unwrap().add_callback(after, callback)
    }
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

#[derive(Debug, Clone)]
pub struct DeviceOutputInfo{
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
        callback_receiver: Receiver<Callback>,
        callback_register: SyncSender<Duration>,
    ) -> JoinHandle<Result<(), StreamErr>>;
    fn get_input_info(&self) -> Result<Cow<'_,StreamerInputInfo>,StreamErr>;
    // TODO *decoded.spec.rate holds always the decoded sample rate, maybe use that as alternative
    fn get_output_info(&self) -> Option<DeviceOutputInfo>;
    fn finished_flag(&self) -> Arc<AtomicBool>;
    fn get_callback_handle(&self) -> StreamerCallBackHandle;
    /// Cloneable transport-control surface (stop/pause/resume/seek/rewind),
    /// safe to capture in a sample callback.
    fn control_handle(&self) -> ControlHandle;
}

#[derive(Clone)]
pub enum Callback {
    CbOnSample(u64),
}
