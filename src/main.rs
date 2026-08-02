use audio_learn::streamer::mixer::Mixer;
use audio_learn::streamer::single::{SingleStreamer, StreamerSource};
use audio_learn::streamer::{Streamer, add_callback};
use std::sync::Arc;
use std::time::Duration;

use audio_learn::stream_player;
use audio_learn::stream_player::BitRateInfo;
use audio_learn::streamer::playlist::{CrossFadeType, PlayListStreamer};
use audio_learn::streamer::utils::f_fadeout_log;

fn main() {
    play_single_streamer();
    //run_playlist();
    //handle.join().unwrap();
    // check_playlist_different_with_mono()
}

fn play_single_streamer() {
    let s3 = SingleStreamer::new(
        StreamerSource::File("./files/well-tempered-clavier-1.mp3".into()),
        "audio/mpeg".to_string(),
    )
    .unwrap();
    let mut player = stream_player::new_stream_player(Box::new(s3), BitRateInfo::Streamer).unwrap();
    let status = player.status(); // cloneable handle: query play time without owning the player
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    add_callback(
        Duration::from_secs(10),
        Box::new(move || {
            let ms2 = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            println!("Callback from single after ,{} seconds", (ms2 - ms) / 1000);
            let play_time = status.get_play_time_ms();
            println!("Play time: from player {}", play_time / 1000f32);
            add_callback(
                Duration::from_secs(15),
                Box::new(move || {
                    println!("Callback from single again!");
                }),
            )
            .unwrap();
        }),
    )
    .unwrap();
    let handle = player.start().unwrap();
    handle.join().unwrap();
}

fn check_playlist_different_with_mono() {
    let s1 = SingleStreamer::new(
        StreamerSource::File("./files/lost_in_the_city.mp3".into()),
        "audio/mpeg".to_string(),
    )
    .unwrap();
    let s2 = SingleStreamer::new(
        StreamerSource::File("./files/mono-sample.mp3".into()),
        "audio/mpeg".to_string(),
    )
    .unwrap();
    let streamers: Vec<Box<dyn Streamer>> = vec![Box::new(s1), Box::new(s2)];
    let play_list = PlayListStreamer::new(streamers, CrossFadeType::Linear(20f32));
    let mut player =
        stream_player::new_stream_player(Box::new(play_list), BitRateInfo::Streamer).unwrap();
    let handle = player.start().unwrap();
    handle.join().unwrap();
}

fn run_playlist() {
    let s1 = SingleStreamer::new(
        StreamerSource::File("./files/long-audio-5min.mp3".into()),
        "audio/mpeg".to_string(),
    )
    .unwrap();
    let s2 = SingleStreamer::new(
        StreamerSource::File("./files/lost_in_the_city.mp3".into()),
        "audio/mpeg".to_string(),
    )
    .unwrap();
    let s3 = SingleStreamer::new(
        StreamerSource::File("./files/well-tempered-clavier-1.mp3".into()),
        "audio/mpeg".to_string(),
    )
    .unwrap();
    let streamers: Vec<Box<dyn Streamer>> = vec![Box::new(s1), Box::new(s2), Box::new(s3)];
    let playList = PlayListStreamer::new(streamers, CrossFadeType::Linear(20f32));

    let mut player =
        stream_player::new_stream_player(Box::new(playList), BitRateInfo::Streamer).unwrap();
    let handle = player.start().unwrap();
    add_callback(
        Duration::from_secs(10),
        Box::new(move || {
            println!("Callback from playlist!");
            add_callback(
                Duration::from_secs(11),
                Box::new(move || {
                    println!("Callback from playlist!");
                }),
            )
            .unwrap();
        }),
    )
    .unwrap();
    handle.join().unwrap();
}
fn run_mixer() {
    let streamer = SingleStreamer::new(
        StreamerSource::File("./files/well-tempered-clavier-1.mp3".into()),
        "audio/mpeg".to_string(),
    )
    .unwrap();
    let s2 = SingleStreamer::new(
        StreamerSource::File("./files/lost_in_the_city.mp3".into()),
        "audio/mpeg".to_string(),
    )
    .unwrap();
    let streamers: Vec<Box<dyn Streamer>> = vec![Box::new(streamer), Box::new(s2)];
    let weights: Vec<u32> = vec![95, 5];
    let mixer = Mixer::new(streamers, weights);
    let mixer_handle = mixer.handle();
    let sample_rate = mixer.get_output_info().unwrap().sample_rate;
    let channels = mixer.get_output_info().unwrap().channels as u32;

    let durSec = 10;
    let samples_in_10s = (sample_rate * channels * durSec) as usize;

    //callback_handle.add_callback(Duration::from_millis(2001), Box::new(|| println!("YOYOYO"))).unwrap_or_else(|e| println!("Error adding callback: {:?}", e));

    add_callback(Duration::from_secs(11), Box::new(|| println!("NOOOOO")))
        .unwrap_or_else(|e| println!("Error adding callback: {:?}", e));
    add_callback(Duration::from_secs(1), Box::new(|| println!("S1 :)")))
        .unwrap_or_else(|e| println!("Error adding callback: {:?}", e));

    let mixer_control = mixer.control_handle();
    let arc_f: Arc<dyn Fn(usize) -> f32 + Send + Sync> =
        Arc::new(move |x| f_fadeout_log(x, samples_in_10s));
    let mxc = mixer_control.clone();
    //let arcF = arcF.clone();
    add_callback(
        Duration::from_secs(4),
        Box::new(move || {
            mxc.add_gain_function(Arc::clone(&arc_f))
                .expect("TODO: panic message");
            ()
        }),
    )
    .expect("TODO: panic message");
    let mut player =
        stream_player::new_stream_player(Box::new(mixer), BitRateInfo::Streamer).unwrap();
    add_callback(
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
