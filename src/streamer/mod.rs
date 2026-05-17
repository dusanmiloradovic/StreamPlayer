pub mod single;

use symphonia::core::io::MediaSource;

pub trait Streamer{
    fn play(self);
    fn pause(self);
    fn stop(self);
    fn seek(self, time:u64);
}


