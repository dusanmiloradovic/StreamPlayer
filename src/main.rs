use audio_learn::streamer::single::SingleStreamer;
use std::fs::File;

pub mod stream_player;

fn main() {
    let file = File::open("./files/well-tempered-clavier-1.mp3").unwrap();
    let streamer = SingleStreamer::new(Box::new(file), "audio/mpeg".to_string()).unwrap();
    let mut player = stream_player::new_stream_player(Box::new(streamer)).unwrap();
    let handle = player.start().unwrap();
    handle.join().unwrap();
}
