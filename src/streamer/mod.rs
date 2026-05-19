use std::error::Error;

pub mod single;

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
    fn push(&mut self, data: &[f32]);
    fn stop(&mut self);

}

pub trait Streamer {
    fn play(self) -> Result<(), StreamErr>;
    fn pause(&mut self) -> Result<(), StreamErr>;
    fn stop(self) -> Result<(), StreamErr>;
    fn seek(self, time: u64) -> Result<(), StreamErr>;
    fn set_sink(&mut self, sink: Box<dyn Sink>);
}
