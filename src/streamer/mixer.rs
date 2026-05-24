use crate::streamer::{StreamErr, Streamer};
use std::sync::mpsc;
use std::sync::mpsc::SyncSender;
use std::sync::mpsc::Receiver;
use std::thread;

pub struct Mixer {
    streamers: Vec<Box<dyn Streamer>>,
    weights: Vec<f32>,
    senders: Vec<Option<SyncSender<Vec<f32>>>>,
    output_sender: Option<SyncSender<Vec<f32>>>,
}

impl Mixer {
    pub fn new(streamers: Vec<Box<dyn Streamer>>, weights: Vec<f32>) -> Self {
        let mut senders = Vec::new();
        if weights.len() != streamers.len() {
            panic!("weights and streamers must have the same length");
        }
        for j in 0..streamers.len() {
            if weights[j] < 0.0 || weights[j] > 1.0 {
                panic!("weights must be between 0.0 and 1.0");
            }
            senders.push(None);
        }
        Self {
            streamers,
            weights,
            senders,
            output_sender: None,
        }
    }
}

impl Streamer for Mixer {
    fn play(&mut self, sender: SyncSender<Vec<f32>>) -> Result<(), StreamErr> {
        let streamers = &mut self.streamers;
        for j in 0..streamers.len() {
            let streamer = &mut streamers[j];
            let (sender, receiver) = mpsc::sync_channel::<Vec<f32>>(8);
            streamer.play(sender)?;
            thread::spawn(move || {
                while let Ok(samples) = receiver.recv() {
                    
                }
            });
        }
        self.output_sender = Some(sender);
        Ok(())
    }

    fn pause(&mut self) -> Result<(), StreamErr> {
        todo!()
    }

    fn resume(&mut self) -> Result<(), StreamErr> {
        todo!()
    }

    fn stop(&self) -> Result<(), StreamErr> {
        todo!()
    }

    fn seek(&self, time: u64) -> Result<(), StreamErr> {
        todo!()
    }

    fn get_input_sample_rate(&self) -> u32 {
        todo!()
    }

    fn get_input_channel_count(&self) -> u16 {
        todo!()
    }
}
