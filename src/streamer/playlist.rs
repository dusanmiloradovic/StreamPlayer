use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::SyncSender;
use std::thread;
use std::thread::{JoinHandle, Thread};
use async_broadcast::Receiver;
use crossbeam_channel::{bounded, Sender};
use crate::streamer::{Callback, ControlCommand, ControlHandle, StreamErr, Streamer, StreamerCallBackHandle, StreamerCallbackShared};


pub enum CrossFadeType{
    Linear(f32),
    None,
    Logarithmic(f32),
}

pub struct PlayListStreamer {
    streamers: Vec<Box<dyn Streamer>>,
    control: ControlHandle,
    control_rx: Option<crossbeam_channel::Receiver<ControlCommand>>,
    sync_tx: Option<Sender<usize>>,
    callback_receiver: Option<Receiver<Callback>>,
    command_rx: Option<crossbeam_channel::Receiver<ControlCommand>>,
    callback_handle: Arc<Mutex<StreamerCallbackShared>>,
    callbacks: Arc<Mutex<HashMap<u64, Box<dyn Fn() + Send>>>>,
    cross_fade_type: CrossFadeType,
}

impl PlayListStreamer {
    pub fn new (streamers: Vec<Box<dyn Streamer>>, cross_fade_type: CrossFadeType) -> Self{
        let (control, control_rx) = ControlHandle::new();
        Self {
            streamers,
            cross_fade_type,
            control,
            control_rx: Some(control_rx),
            sync_tx: None,
            callback_receiver: None,
            command_rx: None,
            callback_handle: Arc::new(Mutex::new(StreamerCallbackShared::new())),
            callbacks: Arc::new(Mutex::new(HashMap::new()))
        }
    }
}

impl Streamer for PlayListStreamer {
    fn play(&mut self, sender: SyncSender<Vec<f32>>, callback_receiver: Receiver<Callback>, callback_register: SyncSender<u64>) -> JoinHandle<Result<(), StreamErr>> {
        if self.streamers.len() == 0 {
            return thread::spawn(move || Ok(()));
        }
        let (a_sender, a_receiver) = bounded::<usize>(8);
        let (b_sender, b_receiver) = bounded::<usize>(8);
        let bounded_senders = [a_sender, b_sender];
        let bounded_receivers = [a_receiver, b_receiver];
        let mut a_current = true;

       let current = self.streamers.remove(0);
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

        Ok(())
    }

    fn get_input_sample_rate(&self) -> u32 {
        self.streamers[0].get_input_sample_rate()
    }

    fn get_input_channel_count(&self) -> u16 {
        self.streamers[0].get_input_channel_count()
    }

    fn get_output_sample_rate(&self) -> u32 {
        self.streamers[0].get_output_sample_rate()
    }

    fn finished_flag(&self) -> Arc<AtomicBool> {
        todo!()
    }

    fn get_callback_handle(&self) -> StreamerCallBackHandle {
        todo!()
    }

    fn control_handle(&self) -> ControlHandle {
        todo!()
    }

    fn get_duration(&self) -> Option<u64> {
        todo!()
    }
}