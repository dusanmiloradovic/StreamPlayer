use std::error::Error;

pub mod single;

#[derive(Debug)]
pub enum StreamErr {
    NoSink,
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
}
pub trait Sink {
    fn push(&mut self, data: &[f32])-> Result<(), StreamErr>;
    fn stop(&mut self)-> Result<(), StreamErr>;
    fn start(&mut self)-> Result<(), StreamErr>;

}

pub trait Streamer {
    fn play(&mut self) -> Result<(), StreamErr>;
    fn pause(&mut self) -> Result<(), StreamErr>;
    fn stop(&self) -> Result<(), StreamErr>;
    fn seek(&self, time: u64) -> Result<(), StreamErr>;
    fn set_sink(&mut self, sink: Box<dyn Sink>);
    fn get_input_sample_rate(&self) -> u32;
    fn get_input_channel_count(&self) -> u16;
}
