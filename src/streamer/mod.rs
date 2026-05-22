use std::sync::mpsc::SyncSender;

pub mod single;

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
}

pub trait Streamer: Send {
    fn play(&mut self, sender: SyncSender<Vec<f32>>) -> Result<(), StreamErr>;
    fn pause(&mut self) -> Result<(), StreamErr>;
    fn resume(&mut self) -> Result<(), StreamErr>;
    fn stop(&self) -> Result<(), StreamErr>;
    fn seek(&self, time: u64) -> Result<(), StreamErr>;
    fn get_input_sample_rate(&self) -> u32;
    fn get_input_channel_count(&self) -> u16;
}
