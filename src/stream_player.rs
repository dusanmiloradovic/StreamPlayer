use async_broadcast::broadcast;
use audio_learn::streamer::Callback;
use audio_learn::streamer::Callback::CbOnSample;
use audio_learn::streamer::{StreamErr, Streamer};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{default_host, StreamError, SupportedStreamConfig};
use ringbuf::{traits::*, HeapRb};
use std::collections::BTreeSet;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

pub struct StreamPlayerImpl {
    default_sample_rate: u32,
    channels: u16,
    config: SupportedStreamConfig,
    streamer: Option<Box<dyn Streamer + Send>>,
    paused: Arc<AtomicBool>,
    command_sender: Option<mpsc::Sender<StreamCommand>>,
    elapsed_samples: Arc<AtomicU64>,
    next_sample_callback: Arc<AtomicU64>,
    sample_callbacks: Arc<Mutex<BTreeSet<u64>>>,
}

pub fn new_stream_player(
    streamer: Box<dyn Streamer + Send>,
) -> Result<StreamPlayerImpl, StreamErr> {
    StreamPlayerImpl::new(streamer)
}

fn push_with_backpressure(producer: &mut ringbuf::HeapProd<f32>, data: &[f32]) {
    let mut offset = 0;
    while offset < data.len() {
        let pushed = producer.push_slice(&data[offset..]);
        offset += pushed;
        if offset < data.len() {
            thread::sleep(std::time::Duration::from_millis(5));
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum StreamCommand {
    Pause,
    Resume,
    Stop,
}

impl StreamPlayerImpl {
    fn new(streamer: Box<dyn Streamer + Send>) -> Result<Self, StreamErr> {
        let channels = streamer.get_input_channel_count();
        let input_sample_rate = streamer.get_input_sample_rate();
        let host = default_host();
        let device = host
            .default_output_device()
            .ok_or(StreamErr::NoOutputDevice)?;

        let config_range = device
            .supported_output_configs()
            .map_err(|_| StreamErr::QueryOutputDeviceError)?
            .find(|c| c.channels() == channels)
            .ok_or(StreamErr::NoDeviceConfigForChannelCount)?;

        let closest = input_sample_rate.clamp(
            config_range.min_sample_rate(),
            config_range.max_sample_rate(),
        );
        let config = config_range.with_sample_rate(closest);
        let default_sample_rate = config.sample_rate();

        Ok(Self {
            channels,
            default_sample_rate,
            config,
            streamer: Some(streamer),
            paused: Arc::new(AtomicBool::new(false)),
            command_sender: None,
            elapsed_samples: Arc::new(AtomicU64::new(0)),
            next_sample_callback: Arc::new(AtomicU64::new(0)),
            sample_callbacks: Arc::new(Mutex::new(BTreeSet::new())),
        })
    }

    pub fn start(&mut self) -> Result<thread::JoinHandle<()>, StreamErr> {
        let (sender, receiver) = mpsc::sync_channel::<Vec<f32>>(8);

        let target_latency_secs = 1f32;
        let raw_size =
            (self.default_sample_rate as f32 * self.channels as f32 * target_latency_secs) as usize;
        let ring_size = raw_size.next_power_of_two();
        let sample_rate = self.default_sample_rate;
        let (mut producer, mut consumer) = HeapRb::<f32>::new(ring_size).split();

        let (command_tx, command_rx) = mpsc::channel::<StreamCommand>();
        let (callback_register_tx, callback_register_rx) = mpsc::sync_channel::<u64>(8);
        let (br_tx, br_rx) = broadcast::<Callback>(8);
        // we use channels in both directions, we need here to register the timings, and
        // the player is responsible for driving the streamers.
        // the streamers themselves are registering the callbacks

        self.command_sender = Some(command_tx);

        // Drains the channel into the ring buffer; after the channel closes, waits
        // for the ring buffer to drain before signalling the cpal keepalive thread.
        let cmd_sender = self.command_sender.as_ref().unwrap().clone();
        let paused = self.paused.clone();
        let counter = self.elapsed_samples.clone();
        let next_sample_callback = self.next_sample_callback.clone();
        let nse2 = self.next_sample_callback.clone();
        let sample_callbacks = self.sample_callbacks.clone();
        thread::spawn(move || {
            while paused.load(Relaxed) {
                thread::sleep(std::time::Duration::from_millis(10));
                // on some platforms, stream.pause() is not working (that is a hardware limitation)
            }
            while let Ok(samples) = receiver.recv() {
                counter.fetch_add(samples.len() as u64, Relaxed);
                push_with_backpressure(&mut producer, &samples);
                let nse = next_sample_callback.load(Relaxed);
                let l = counter.load(Relaxed);
                if nse != 0 && l >= nse {
                    br_tx.try_broadcast(CbOnSample(nse)).ok();
                    let b =sample_callbacks.lock().unwrap().pop_first();
                    if let Some(v)=b{
                        next_sample_callback.store(v, Relaxed);
                    }else {
                        next_sample_callback.store(0, Relaxed);
                    }
                }
            }
            let drain_time =
                std::time::Duration::from_secs_f32(ring_size as f32 / sample_rate as f32);
            thread::sleep(drain_time);
            cmd_sender.send(StreamCommand::Stop).unwrap();
        });

        let counter = self.elapsed_samples.clone();
        let sample_callbacks = self.sample_callbacks.clone();
        thread::spawn(move || {
            while let Ok(callback) = callback_register_rx.recv() {
                let elapsed_samples = counter.load(Relaxed);
                if callback < elapsed_samples {
                    return;
                }
                let nse = nse2.load(Relaxed);
                if nse == 0 {
                    nse2.store(callback, Relaxed);
                } else {
                    sample_callbacks.lock().unwrap().insert(callback);
                }
            }
        });

        let data_callback = move |out: &mut [f32], _: &cpal::OutputCallbackInfo| {
            for sample in out.iter_mut() {
                *sample = consumer.try_pop().unwrap_or(0.0);
            }
        };
        let err_fn = |err: StreamError| eprintln!("stream error: {err}");
        let device = default_host()
            .default_output_device()
            .ok_or(StreamErr::NoOutputDevice)?;
        let stream = device
            .build_output_stream(&self.config.config(), data_callback, err_fn, None)
            .map_err(|_| StreamErr::OutputStreamError)?;
        stream.play().map_err(|_| StreamErr::OutputStreamError)?;
        let mut streamer = self.streamer.take().expect("start() called twice");
        let handle = thread::spawn(move || {
            if let Err(e) = streamer
                .play(sender, br_rx.clone(), callback_register_tx)
                .join()
                .unwrap_or(Err(StreamErr::UnknownError))
            {
                eprintln!("playback error: {e:?}");
            }
            // sender dropped here → channel closes → consumer thread exits after drain
        });
        let paused = self.paused.clone();

        thread::spawn(move || {
            let _stream = stream;
            //holds the stream alive until the stop command is received
            while let Ok(command) = command_rx.recv() {
                if command == StreamCommand::Pause {
                    _stream
                        .pause()
                        .unwrap_or_else(|_| eprintln!("pause failed"));
                    paused.store(true, Relaxed);
                }
                if command == StreamCommand::Resume {
                    _stream
                        .play()
                        .unwrap_or_else(|_| eprintln!("resume failed"));
                    paused.store(false, Relaxed);
                }
                if command == StreamCommand::Stop {
                    break;
                }
            }
        });

        Ok(handle)
    }

    pub fn pause(&mut self) -> Result<(), StreamErr> {
        let command_sender = self.command_sender.as_ref().unwrap();
        command_sender.send(StreamCommand::Pause).unwrap();
        self.streamer.as_mut().unwrap().pause()
    }

    pub fn resume(&mut self) -> Result<(), StreamErr> {
        let command_sender = self.command_sender.as_ref().unwrap();
        command_sender.send(StreamCommand::Resume).unwrap();
        self.streamer.as_mut().unwrap().resume()
    }

    pub fn stop(&mut self) -> Result<(), StreamErr> {
        let command_sender = self.command_sender.as_ref().unwrap();
        command_sender.send(StreamCommand::Stop).unwrap();
        self.streamer.take().unwrap().stop()
    }

    pub fn get_play_time_ms(&self) -> f32 {
        let elapsed_samples = self.elapsed_samples.load(Relaxed);
        let elapsed_ms = elapsed_samples as f32
            / (self.default_sample_rate as f32 * self.channels as f32)
            * 1000.0;
        elapsed_ms
    }
}
