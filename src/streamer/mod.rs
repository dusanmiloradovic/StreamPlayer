use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::Arc;
use std::thread::JoinHandle;

pub mod mixer;
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
}

pub trait Streamer: Send {
    fn play(
        &mut self,
        sender: SyncSender<Vec<f32>>,
        callback_receiver: Receiver<Callback>,
        callback_register: SyncSender<u64>,
    ) -> JoinHandle<Result<(), StreamErr>>;
    fn pause(&mut self) -> Result<(), StreamErr>;
    fn resume(&mut self) -> Result<(), StreamErr>;
    fn stop(&self) -> Result<(), StreamErr>;
    fn seek(&self, time: u64) -> Result<(), StreamErr>;
    fn rewind(&self) -> Result<(), StreamErr>;
    fn get_input_sample_rate(&self) -> u32;
    fn get_input_channel_count(&self) -> u16;
    fn get_output_sample_rate(&self) -> u32; // the output sample rate should match the closest supported sample rate, and the stream should be resampled to this rate.
    fn finished_flag(&self) -> Arc<AtomicBool>;
    fn add_callback(&mut self, callback_time: u64, callback: Box<dyn Fn() + Send>);
}

pub enum Callback {
    Callback(u64),
}
