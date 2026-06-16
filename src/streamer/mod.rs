use async_broadcast::Receiver;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

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

pub struct StreamerCallbackHandle{
    sample_rate:u32,
    channel_count:u32,
    callback_register:Option<SyncSender<u64>>,
    pending_callbacks:Arc<Mutex<Vec<u64>>>,
}

impl StreamerCallbackHandle {

    fn add_callback(&self, after: Duration, callback: Box<dyn Fn() + Send>) {
        let secs = after.as_secs();
        let samples =
            secs * self.sample_rate as u64 * self.channel_count as u64;
        let mut pending_callbacks = self.pending_callbacks.lock().unwrap();
        match &self.callback_register{
            None=>{
                pending_callbacks.push(samples);
            }
            Some(cr)=>{
                cr.send(samples).unwrap();
            }
        }
    }
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
    fn get_callback_receiver(&self) -> Option<Receiver<Callback>>;
    fn get_callback_register(&self) -> Option<SyncSender<u64>>;

    fn callback_register(&self) -> &Option<SyncSender<u64>>;
    fn callbacks(&self) -> &Arc<Mutex<HashMap<u64, Box<dyn Fn() + Send>>>>;
    fn get_callback_handle(&self) -> Arc<Mutex<StreamerCallbackHandle>>;
    
}

#[derive(Clone)]
pub enum Callback {
    CbOnSample(u64),
}
