use audio_learn::streamer::mixer::Mixer;
use audio_learn::streamer::single::SingleStreamer;
use audio_learn::streamer::Streamer;
use std::fs::File;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use audio_learn::stream_player;

fn main() {
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
    let sample_rate = mixer.get_output_sample_rate();
    let channels = mixer.get_input_channel_count() as u32;
    // TODO what happens when we have a mono channel streamer?
    let durSec = 10;
    let samples_in_10s = (sample_rate * channels * durSec) as usize;

    let linear_fadeout = move |x: usize| {
        if x < samples_in_10s {
           return (samples_in_10s - x) as f32 / samples_in_10s as f32;
            //return 0.75;
        }
        return 0.0;
    };

    let fadeout_log = move |x: usize| {
        if x >= samples_in_10s {
            return 0.0;
        }
        let t = x as f32 / samples_in_10s as f32;
        let floor_db = -60.0_f32;
        let db = floor_db * t;
        10.0_f32.powf(db / 20.0)
    };

    //callback_handle.add_callback(Duration::from_millis(2001), Box::new(|| println!("YOYOYO"))).unwrap_or_else(|e| println!("Error adding callback: {:?}", e));

    s2_callback_handle
        .add_callback(Duration::from_secs(11), Box::new(|| println!("NOOOOO")))
        .unwrap_or_else(|e| println!("Error adding callback: {:?}", e));
    s1_callback_handle
        .add_callback(Duration::from_secs(1), Box::new(|| println!("S1 :)")))
        .unwrap_or_else(|e| println!("Error adding callback: {:?}", e));



    let mixer_control = mixer.control_handle();
    let arc_f = Arc::new(fadeout_log);
    let mxc = mixer_control.clone();
    //let arcF = arcF.clone();
    callback_handle
        .add_callback(Duration::from_secs(4), Box::new(move|| {
            mxc.add_gain_function(arc_f.clone()).expect("TODO: panic message");
            ()
        })).expect("TODO: panic message");
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
