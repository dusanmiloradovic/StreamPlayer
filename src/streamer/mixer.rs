use crate::streamer::{StreamErr, Streamer};
use ringbuf::{CachingCons, CachingProd, HeapRb, SharedRb};
use ringbuf::traits::Split;
use std::sync::atomic::{AtomicU8, AtomicUsize};
use std::sync::mpsc::Receiver;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, mpsc};
use std::thread;
use std::thread::JoinHandle;
use ringbuf::producer::Producer;
use ringbuf::traits::Consumer;
use ringbuf::storage::Heap;

pub struct Mixer {
    streamers: Vec<Box<dyn Streamer>>,
    weights: Vec<f32>,
}

const FRAME_MAX_DELAY: u8 = 10; // wait for all frames to sync up the count. If one or more frames
//fail to load any data during this period, the mixer will mix up with empty values

impl Mixer {
    pub fn new(streamers: Vec<Box<dyn Streamer>>, weights: Vec<f32>) -> Self {
        if weights.len() != streamers.len() {
            panic!("weights and streamers must have the same length");
        }
        let len = streamers.len();
        for j in 0..len {
            if weights[j] < 0.0 || weights[j] > 1.0 {
                panic!("weights must be between 0.0 and 1.0");
            }
        }

        // TODO check all output sample rates are the same.
        Self {
            streamers,
            weights,
        }
    }
}

struct ChannelData {
    index: usize,
    data: Vec<f32>,
}

impl Streamer for Mixer {
    fn play(&mut self, sender: SyncSender<Vec<f32>>) -> Result<JoinHandle<Result<(), StreamErr>>, StreamErr> {

        let target_latency_secs = 1usize; // TODO make this constant across the project
        let ring_size = self.get_output_sample_rate() as usize
            * self.get_input_channel_count() as usize
            * target_latency_secs;
        let channel_count = self.get_input_channel_count();
        let weights = self.weights.clone();
        let streamers = &mut self.streamers;
        let streamers_len = streamers.len();


        let indices = vec![Arc::new(AtomicUsize::new(0)); streamers_len];
        let frame_counters = vec![Arc::new(AtomicUsize::new(0)); streamers_len];
        let mut ring_producers:Vec<CachingProd<Arc<SharedRb<Heap<f32>>>>>= Vec::new();
        let mut ring_consumers:Vec<CachingCons<Arc<SharedRb<Heap<f32>>>>> = Vec::new();
        for _ in 0..streamers.len() {
            let (producer, consumer) = HeapRb::<f32>::new(ring_size).split();
            ring_producers.push(producer);
            ring_consumers.push(consumer);
        }

        let (sync_sender, sync_receiver) = mpsc::sync_channel::<usize>(8); // signals which stream data was sent



        for j in 0..streamers.len() {
            let streamer = &mut streamers[j];
            let (sender, receiver) = mpsc::sync_channel::<Vec<f32>>(8);
            streamer.play(sender)?;
            let atomic_index = indices[j].clone();
            let atomic_counter = frame_counters[j].clone();
            let  mut ring_producer = ring_producers.remove(0);
            let ssender = sync_sender.clone();
            thread::spawn( move || {
                while let Ok(samples) = receiver.recv() {

                    atomic_counter.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                    atomic_index
                        .fetch_add(samples.len(), std::sync::atomic::Ordering::AcqRel);
                    ring_producer.push_slice(&samples);
                    ssender.send(j).unwrap();
                }
            });
        }

        Ok(thread::spawn(move || -> Result<(), StreamErr> {
            let mut local_ring_consumers = ring_consumers.drain(..).collect::<Vec<_>>();
            let mut min_counter = streamers_len;
            let mut max_counter = 0;
            let mut min_index = usize::MAX;
            let mut prev_index = 0; // index is ever-increasing, we need to calculate the diff
            while let Ok(index) = sync_receiver.recv() {
                for j in 0..streamers_len {
                    let atomic_index = indices[j].clone();
                    let atomic_counter = frame_counters[j].clone();
                    let index = atomic_index.load(std::sync::atomic::Ordering::Acquire);
                    let counter = atomic_counter.load(std::sync::atomic::Ordering::Acquire);
                    min_counter = min_counter.min(counter);
                    max_counter = max_counter.max(counter);
                    min_index = min_index.min(index);
                }
                if min_counter == max_counter {  //all the threads processed frames
                    let v_size = min_index - prev_index;
                    prev_index = min_index;
                    let mut output_data:Vec<f32> = vec![0.0;channel_count as usize * v_size];
                    let koef = 1.0 / weights.iter().sum::<f32>();

                    for j in 0..streamers_len {
                        let cons = &mut local_ring_consumers[j];
                        let  mut v:Vec<f32> = vec![0.0;channel_count as usize * v_size ];
                        cons.pop_slice(&mut v);
                        output_data.iter_mut().zip(v.iter()).for_each(|(a,b)| *a += b * weights[j] * koef);
                    }
                    sender.send(output_data).unwrap();
                } // TODO else if the max diff is larger than FRAME_MAX_DELAY, we need to drop some frames
            }
            Ok(())
        }))
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
        self.streamers[0].get_input_sample_rate()
    }

    fn get_input_channel_count(&self) -> u16 {
        self.streamers[0].get_input_channel_count()
    }

    fn get_output_sample_rate(&self) -> u32 {
        self.streamers[0].get_output_sample_rate() //all guaranteed to have the same sample rate
    }
}
