use audio_learn::streamer::single::SingleStreamer;
use std::fs::File;
use std::thread;
use std::time::Duration;
use audio_learn::streamer::mixer::Mixer;
use audio_learn::streamer::Streamer;

pub mod stream_player;

fn main() {
    let file = File::open("./files/well-tempered-clavier-1.mp3").unwrap();
    let streamer = SingleStreamer::new(Box::new(file), "audio/mpeg".to_string()).unwrap();
    let f2 = File::open("./files/lost_in_the_city.mp3").unwrap();
    let s2 = SingleStreamer::new(Box::new(f2), "audio/mpeg".to_string()).unwrap();
    let s2_callback_handle = &s2.get_callback_handle();
    let s1_callback_handle = &streamer.get_callback_handle();
    let streamers:Vec<Box<dyn Streamer>> = vec![Box::new(streamer),Box::new(s2)];
    let weights: Vec<u32> = vec![95, 5];
    let mixer = Mixer::new(streamers,weights);
    let mixer_handle = mixer.handle();
    let callback_handle = mixer.get_callback_handle();


    callback_handle.add_callback(Duration::from_secs(2), Box::new(|| println!("YOYOYO")));
    s2_callback_handle.add_callback(Duration::from_secs(11), Box::new(|| println!("NOOOOO")));
    s1_callback_handle.add_callback(Duration::from_secs(1), Box::new(|| println!("S1 :)")));
    let mut player = stream_player::new_stream_player(Box::new(mixer)).unwrap();
    let handle = player.start().unwrap();
    let file = File::open("./files/well-tempered-clavier-1.mp3").unwrap();
    let streamer2 = SingleStreamer::new(Box::new(file), "audio/mpeg".to_string()).unwrap();
    thread::sleep(Duration::from_secs(7));
    println!("added");
    mixer_handle.add(Box::new(streamer2), 100, true);
   // streamer.add_callback(Default::default(), Box::new(|| println!("added")));
    handle.join().unwrap();


    // let file = File::open("./files/well-tempered-clavier-1.mp3").unwrap();
    // let streamer = SingleStreamer::new(Box::new(file), "audio/mpeg".to_string()).unwrap();
    // let mut player = stream_player::new_stream_player(Box::new(streamer)).unwrap();
    // let handle = player.start().unwrap();
    // handle.join().unwrap();

    // let file = File::open("./files/well-tempered-clavier-1.mp3").unwrap();
    // let streamer = SingleStreamer::new(Box::new(file), "audio/mpeg".to_string()).unwrap();
    // let streamers:Vec<Box<dyn Streamer>> = vec![Box::new(streamer)];
    // let weights:Vec<f32>=vec![1.0];
    // let mixer = Mixer::new(streamers,weights);
    // let mut player = stream_player::new_stream_player(Box::new(mixer)).unwrap();
    // let handle = player.start().unwrap();
    //handle.join().unwrap();
}
