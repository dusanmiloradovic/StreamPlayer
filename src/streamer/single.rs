use symphonia::core::io::MediaSource;
use crate::streamer::Streamer;

struct SingleStreamer{
    media_source:Box<dyn MediaSource>,
}

impl SingleStreamer {
    pub fn new(source: Box<dyn MediaSource>) -> Self {
        Self { media_source: source }
    }
}

impl Streamer for SingleStreamer{
    fn play(self) {
        todo!()
    }

    fn pause(self) {
        todo!()
    }

    fn stop(self) {
        todo!()
    }

    fn seek(self, time: u64) {
        todo!()
    }
}

