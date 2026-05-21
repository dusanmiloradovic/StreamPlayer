use audio_learn::streamer::Sink;
use audio_learn::streamer::Streamer;
use audio_learn::streamer::single::SingleStreamer;
use std::fs::File;

pub mod stream_player;

fn main() {
    // play_sample_on_device().expect("failed");
    let file = File::open("./files/well-tempered-clavier-1.mp3").unwrap();
    let mut streamer = SingleStreamer::new(Box::new(file), "audio/mpeg".to_string()).unwrap();
    let mut boxed_streamer = Box::new(streamer);
    let mut player = stream_player::new_stream_player(boxed_streamer.as_ref()).unwrap();
    player.start();

    boxed_streamer.set_sink(Box::new(player));
    boxed_streamer.play().unwrap();

}
