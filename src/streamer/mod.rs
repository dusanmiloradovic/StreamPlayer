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

    fn get_sink(&self) -> Option<Box<dyn Sink>>; // this goes to the next stage, at minimum there will be player
}
