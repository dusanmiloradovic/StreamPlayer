use crate::streamer::{StreamErr, Streamer};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::thread::JoinHandle;

pub struct Mixer {
    streamers: Vec<Box<dyn Streamer>>,
    weights: Vec<f32>,
    finished: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
}

impl Mixer {
    pub fn new(streamers: Vec<Box<dyn Streamer>>, weights: Vec<f32>) -> Self {
        if weights.len() != streamers.len() {
            panic!("weights and streamers must have the same length");
        }
        for &w in &weights {
            if w < 0.0 {
                panic!("weights must be positive");
            }
        }
        // TODO check all output sample rates are the same.
        Self {
            streamers,
            weights,
            finished: Arc::new(AtomicBool::new(false)),
            stopped: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Streamer for Mixer {
    fn play(&mut self, sender: SyncSender<Vec<f32>>) -> JoinHandle<Result<(), StreamErr>> {
        let weights = self.weights.clone();
        let streamers = &mut self.streamers;
        let streamers_len = streamers.len();

        let indices: Vec<Arc<AtomicUsize>> = (0..streamers_len)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect();
        let shared_bufs: Vec<Arc<Mutex<VecDeque<f32>>>> = (0..streamers_len)
            .map(|_| Arc::new(Mutex::new(VecDeque::new())))
            .collect();

        let finished_flags: Vec<Arc<AtomicBool>> =
            streamers.iter().map(|s| s.finished_flag()).collect();

        let (sync_sender, sync_receiver) = mpsc::sync_channel::<usize>(8);

        for j in 0..streamers.len() {
            let streamer = &mut streamers[j];
            let (inner_sender, inner_receiver) = mpsc::sync_channel::<Vec<f32>>(8);
            streamer.play(inner_sender);
            let atomic_index = indices[j].clone();
            let shared_buf = shared_bufs[j].clone();
            let ssender = sync_sender.clone();
            let finished_flag = finished_flags[j].clone();
            thread::spawn(move || {
                while let Ok(samples) = inner_receiver.recv()
                    && !finished_flag.load(std::sync::atomic::Ordering::Acquire)
                {
                    shared_buf.lock().unwrap().extend(samples.iter().copied());
                    atomic_index.fetch_add(samples.len(), std::sync::atomic::Ordering::AcqRel);
                    ssender.send(j).unwrap();
                }
            });
        }

        let finished = self.finished.clone();
        let stopped = self.stopped.clone();
        thread::spawn(move || -> Result<(), StreamErr> {
            let mut min_index;
            let mut prev_index = 0usize;
            while let Ok(_) = sync_receiver.recv() && !stopped.load(std::sync::atomic::Ordering::Acquire) {
                min_index = usize::MAX;
                for j in 0..streamers_len {
                    if finished_flags[j].load(std::sync::atomic::Ordering::Acquire) {
                        continue;
                    }
                    let index = indices[j].load(std::sync::atomic::Ordering::Acquire);
                    min_index = min_index.min(index);
                }
                if min_index == usize::MAX {
                    // All streamers finished
                    break;
                }
                let v_size = min_index - prev_index;
                if v_size > 0 {
                    prev_index = min_index;
                    let koef = 1.0 / weights.iter().sum::<f32>();
                    let mut output_data = vec![0.0f32; v_size];
                    for j in 0..streamers_len {
                        let mut buf = shared_bufs[j].lock().unwrap();
                        let available = buf.len().min(v_size);
                        if available == 0 {
                            continue;
                        }
                        for (out, s) in output_data.iter_mut().zip(buf.drain(..available)) {
                            *out += s * weights[j] * koef;
                        }
                    }
                    sender.send(output_data).map_err(|_| StreamErr::SendError)?;
                }
            }
            finished.store(true, std::sync::atomic::Ordering::Release);
            Ok(())
        })
    }

    fn pause(&mut self) -> Result<(), StreamErr> {
        self.streamers.iter_mut().try_for_each(|s| s.pause())
    }

    fn resume(&mut self) -> Result<(), StreamErr> {
        self.streamers.iter_mut().try_for_each(|s| s.resume())
    }

    fn stop(&self) -> Result<(), StreamErr> {
        let stopped = self.stopped.clone();
        stopped.store(true, std::sync::atomic::Ordering::Relaxed);
        self.streamers.iter().try_for_each(|s| s.stop())
    }

    fn seek(&self, time: u64) -> Result<(), StreamErr> {
        self.streamers.iter().try_for_each(|s| s.seek(time))
    }

    fn rewind(&self) -> Result<(), StreamErr> {
        self.streamers.iter().try_for_each(|s| s.rewind())
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
        self.finished.clone()
    }
}
