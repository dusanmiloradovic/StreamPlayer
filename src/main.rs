use audio_learn::streamer::mixer::Mixer;
use audio_learn::streamer::single::SingleStreamer;
use audio_learn::streamer::Streamer;
use std::fs::File;
use std::sync::Arc;
use std::time::Duration;

use audio_learn::stream_player;
use audio_learn::streamer::playlist::{CrossFadeType, PlayListStreamer};
use audio_learn::streamer::utils::f_fadeout_log;

fn main() {
   // run_playlist();
    //handle.join().unwrap();
    check_playlist_different_with_mono()
}

fn check_playlist_different_with_mono(){
    let f1 = File::open("./files/lost_in_the_city.mp3").unwrap();
    let f2= File::open("./files/mono-sample.mp3").unwrap();
    let s1 = SingleStreamer::new(Box::new(f1), "audio/mpeg".to_string()).unwrap();
    let s2 = SingleStreamer::new(Box::new(f2), "audio/mpeg".to_string()).unwrap();
    let streamers: Vec<Box<dyn Streamer>> = vec![Box::new(s1), Box::new(s2)];
    let play_list = PlayListStreamer::new(streamers, CrossFadeType::Linear(20f32));
    let mut player = stream_player::new_stream_player(Box::new(play_list)).unwrap();
    let handle = player.start().unwrap();
    handle.join().unwrap();
}

fn run_playlist() {
    let f3 = File::open("./files/well-tempered-clavier-1.mp3").unwrap();
    let f2 = File::open("./files/lost_in_the_city.mp3").unwrap();
    let f1 = File::open("./files/long-audio-5min.mp3").unwrap();
    let s1 = SingleStreamer::new(Box::new(f1), "audio/mpeg".to_string()).unwrap();
    let s2 = SingleStreamer::new(Box::new(f2), "audio/mpeg".to_string()).unwrap();
    let s3 = SingleStreamer::new(Box::new(f3), "audio/mpeg".to_string()).unwrap();
    let streamers: Vec<Box<dyn Streamer>> = vec![Box::new(s1), Box::new(s2), Box::new(s3)];
    let playList = PlayListStreamer::new(streamers, CrossFadeType::Linear(20f32));
    let callback_handle = playList.get_callback_handle();
    let cbh = playList.get_callback_handle();

    let mut player = stream_player::new_stream_player(Box::new(playList)).unwrap();
    let handle = player.start().unwrap();
    callback_handle
        .add_callback(
            Duration::from_secs(10),
            Box::new(move|| {
                println!("Callback from playlist!");
                cbh.add_callback(Duration::from_secs(11), Box::new(move|| {
                    println!("Callback from playlist!");

                })).unwrap();
            }),
        )
        .unwrap();
    handle.join().unwrap();
}
fn run_mixer() {
    let file = File::open("./files/well-tempered-clavier-1.mp3").unwrap();
    let streamer = SingleStreamer::new(Box::new(file), "audio/mpeg".to_string()).unwrap();
    let f2 = File::open("./files/lost_in_the_city.mp3").unwrap();
    let s2 = SingleStreamer::new(Box::new(f2), "audio/mpeg".to_string()).unwrap();
    let s2_callback_handle = &s2.get_callback_handle();
    let s1_callback_handle = &streamer.get_callback_handle();
    let streamers: Vec<Box<dyn Streamer>> = vec![Box::new(streamer), Box::new(s2)];
    let weights: Vec<u32> = vec![95, 5];
    let mixer = Mixer::new(streamers, weights);
    let mixer_handle = mixer.handle();
    let callback_handle = mixer.get_callback_handle();
    let sample_rate = mixer.get_output_info().unwrap().sample_rate;
    let channels = mixer.get_output_info().unwrap().channels as u32;

    let durSec = 10;
    let samples_in_10s = (sample_rate * channels * durSec) as usize;

    //callback_handle.add_callback(Duration::from_millis(2001), Box::new(|| println!("YOYOYO"))).unwrap_or_else(|e| println!("Error adding callback: {:?}", e));

    s2_callback_handle
        .add_callback(Duration::from_secs(11), Box::new(|| println!("NOOOOO")))
        .unwrap_or_else(|e| println!("Error adding callback: {:?}", e));
    s1_callback_handle
        .add_callback(Duration::from_secs(1), Box::new(|| println!("S1 :)")))
        .unwrap_or_else(|e| println!("Error adding callback: {:?}", e));

    let mixer_control = mixer.control_handle();
    let arc_f = Arc::new(move |x| f_fadeout_log(x, samples_in_10s));
    let mxc = mixer_control.clone();
    //let arcF = arcF.clone();
    callback_handle
        .add_callback(
            Duration::from_secs(4),
            Box::new(move || {
                mxc.add_gain_function(arc_f.clone())
                    .expect("TODO: panic message");
                ()
            }),
        )
        .expect("TODO: panic message");
    let mut player = stream_player::new_stream_player(Box::new(mixer)).unwrap();
    callback_handle
        .add_callback(
            Duration::from_secs(25),
            Box::new(move || {
                mixer_control
                    .stop()
                    .unwrap_or_else(|e| println!("Error stopping mixer: {:?}", e))
            }),
        )
        .unwrap_or_else(|e| println!("Error adding callback: {:?}", e));

    let handle = player.start().unwrap();
    // let file = File::open("./files/well-tempered-clavier-1.mp3").unwrap();
    // let streamer2 = SingleStreamer::new(Box::new(file), "audio/mpeg".to_string()).unwrap();
    // thread::sleep(Duration::from_secs(7));
    // println!("added");
    // mixer_handle.add(Box::new(streamer2), 100, true);
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
