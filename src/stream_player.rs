use crate::streamer::Callback::CbOnSample;
use crate::streamer::{Callback, DeviceOutputInfo, NO_SEEK, callback_shared};
use crate::streamer::{StreamErr, Streamer};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{StreamError, SupportedStreamConfig, default_host};
use ringbuf::{HeapRb, traits::*};
use std::collections::BTreeSet;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

pub struct StreamPlayerImpl {
    config: SupportedStreamConfig,
    streamer: Option<Box<dyn Streamer + Send>>,
    paused: Arc<AtomicBool>,
    command_sender: Option<mpsc::Sender<StreamCommand>>,
    elapsed_samples: Arc<AtomicU64>,
    media_elapsed_samples: Arc<AtomicU64>, // this is set with seek, and it will be used in callbacks.
    //TODO next version both elapsed_samples and media_elapsed_samples will be used in callbacks. (depending on config)
    next_sample_callback: Arc<AtomicU64>,
    sample_callbacks: Arc<Mutex<BTreeSet<u64>>>,
    device_output_info: DeviceOutputInfo,
    last_seek_position: Arc<AtomicU64>,
}

pub fn new_stream_player(
    streamer: Box<dyn Streamer + Send>,
    bit_rate_info: BitRateInfo,
) -> Result<StreamPlayerImpl, StreamErr> {
    StreamPlayerImpl::new(streamer, bit_rate_info)
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

pub enum BitRateInfo {
    Streamer,
    DeviceDefault,
    Manual(u32, u16),
}

// currently, the commands are sent directly to streamers
// This is the only exception, we need to adjust the  callback timings after the seek on streamer
pub(crate) enum StreamNotify {
    Seek(f64),// we need also fractional parts of seconds
    Rewind,
}

impl StreamPlayerImpl {
    fn new(
        streamer: Box<dyn Streamer + Send>,
        bit_rate_info: BitRateInfo,
    ) -> Result<Self, StreamErr> {
        let input_info = streamer.get_input_info();
        let host = default_host();
        let device = host
            .default_output_device()
            .ok_or(StreamErr::NoOutputDevice)?;
        let default_output_config = device
            .default_output_config()
            .map_err(|_| StreamErr::QueryOutputDeviceError)?;
        let (sample_rate, channels) = match bit_rate_info {
            BitRateInfo::Manual(sample_rate, channels) => (sample_rate, channels),
            BitRateInfo::DeviceDefault => (
                default_output_config.sample_rate(),
                default_output_config.channels(),
            ),
            BitRateInfo::Streamer => {
                if input_info.is_err() {
                    (
                        default_output_config.sample_rate(),
                        default_output_config.channels(),
                    )
                } else {
                    let ii = input_info?;
                    (ii.sample_rate, ii.channels)
                }
            }
        };

        let config_range = device
            .supported_output_configs()
            .map_err(|_| StreamErr::QueryOutputDeviceError)?
            .find(|c| c.channels() == channels)
            .ok_or(StreamErr::NoDeviceConfigForChannelCount)?;

        let closest = sample_rate.clamp(
            config_range.min_sample_rate(),
            config_range.max_sample_rate(),
        );
        let config = config_range.with_sample_rate(closest);
        let default_sample_rate = config.sample_rate();
        let last_seek_position = streamer.last_seek_position();

        Ok(Self {
            device_output_info: DeviceOutputInfo {
                sample_rate: default_sample_rate,
                channels,
            },
            config,
            streamer: Some(streamer),
            paused: Arc::new(AtomicBool::new(false)),
            command_sender: None,
            elapsed_samples: Arc::new(AtomicU64::new(0)),
            media_elapsed_samples: Arc::new(AtomicU64::new(0)),
            next_sample_callback: Arc::new(AtomicU64::new(0)),
            sample_callbacks: Arc::new(Mutex::new(BTreeSet::new())),
            last_seek_position,
        })
    }

    pub fn start(&mut self) -> Result<thread::JoinHandle<()>, StreamErr> {
        let (sender, receiver) = mpsc::sync_channel::<Vec<f32>>(8);

        let target_latency_secs = 1f32;
        let raw_size = (self.device_output_info.sample_rate as f32
            * self.device_output_info.channels as f32
            * target_latency_secs) as usize;
        let ring_size = raw_size.next_power_of_two();
        let sample_rate = self.device_output_info.sample_rate;
        let (mut producer, mut consumer) = HeapRb::<f32>::new(ring_size).split();

        let (command_tx, command_rx) = mpsc::channel::<StreamCommand>();
        let (notifer_tx, notifer_rx) = mpsc::sync_channel::<StreamNotify>(8);
        let (callback_register_tx, callback_register_rx) = mpsc::sync_channel::<u64>(8);
        let (callback_sender, callback_receiver) = mpsc::sync_channel::<Callback>(8);

        // we use channels in both directions, we need here to register the timings, and
        // the player is responsible for driving the streamers.
        // the streamers themselves are registering the callbacks

        self.command_sender = Some(command_tx);
        let streamer_last_seek_position = Arc::clone(&self.last_seek_position);

        // Drains the channel into the ring buffer; after the channel closes, waits
        // for the ring buffer to drain before signalling the cpal keepalive thread.
        let cmd_sender = self.command_sender.as_ref().unwrap().clone();
        let paused = Arc::clone(&self.paused);
        let stopped = Arc::new(AtomicBool::new(false));
        let passed_callbacks: Arc<Mutex<BTreeSet<u64>>> = Arc::new(Mutex::new(BTreeSet::new()));
        let pending_seek_buffer_clear = Arc::new(AtomicU64::new(NO_SEEK));
        // we need to clear the buffer on seek and set the position in data_callback to avoid race condition

        // since we can go back and forth in time we need to keep the passed callbacks and insert them into sample_callbacks when required
        thread::spawn(move || {
            while paused.load(Relaxed) {
                thread::sleep(std::time::Duration::from_millis(10));
                // on some platforms, stream.pause() is not working (that is a hardware limitation)
            }
            while let Ok(samples) = receiver.recv() {
                push_with_backpressure(&mut producer, &samples);
            }
            let drain_time = Duration::from_secs_f32(ring_size as f32 / sample_rate as f32);
            thread::sleep(drain_time);
            cmd_sender.send(StreamCommand::Stop).unwrap();
        });

        let counter = Arc::clone(&self.elapsed_samples);
        let sample_callbacks = Arc::clone(&self.sample_callbacks);
        let nse2 = Arc::clone(&self.next_sample_callback);
        thread::spawn(move || {
            while let Ok(callback) = callback_register_rx.recv() {
                let elapsed_samples = counter.load(Relaxed);
                if callback < elapsed_samples {
                    continue;
                }
                let nse = nse2.load(Relaxed);
                if nse == 0 {
                    nse2.store(callback, Relaxed);
                } else {
                    if callback < nse {
                        nse2.store(callback, Relaxed);
                        sample_callbacks.lock().unwrap().insert(nse);
                        continue;
                    }
                    sample_callbacks.lock().unwrap().insert(callback);
                }
            }
        });
        let dio = self.device_output_info;
        let sample_rate = dio.sample_rate;
        let channels = dio.channels;
        println!("sample_rate: {}, channels: {}", sample_rate, channels);
       // let seek_media_counter = Arc::clone(&self.media_elapsed_samples);
        let nse = Arc::clone(&self.next_sample_callback);
        //let passed_callbacks = Arc::clone(&self.passed_callbacks);
        let psd = Arc::clone(&passed_callbacks);
        let smpl = Arc::clone(&self.sample_callbacks);
        let pending_seek_buffer_clear = Arc::clone(&pending_seek_buffer_clear);
        let psb = Arc::clone(&pending_seek_buffer_clear);
        // We need to adjust media counter from another thread (data_callback)
        thread::spawn(move || {
            while let Ok(notification) = notifer_rx.recv() {
                if let StreamNotify::Seek(secs) = notification {
                    let mut psdl = psd.lock().unwrap();
                    let mut smpl = smpl.lock().unwrap();

                    if streamer_last_seek_position.load(Relaxed) != NO_SEEK {
                        streamer_last_seek_position.store(NO_SEEK, Relaxed);
                        // there will be multiple streamers notifying , only the main one will be effective
                        // but we need to reset this
                        let new_media_samples_pos = (channels as f64 * sample_rate as f64 * secs) as u64;
                        pending_seek_buffer_clear.store(new_media_samples_pos, Relaxed);
                       // seek_media_counter.store(new_media_samples_pos, Relaxed);
                        // In the audio thread (data_callback) we will set the counter, that is where its increased
                        //NOW handle th eposition
                        let next_counter = nse.load(Relaxed);
                       // let smc = seek_media_counter.load(Relaxed);
                        let smc = new_media_samples_pos;
                        if next_counter <= smc {
                            // already passed the triggering time
                            nse.store(0, Relaxed);
                            println!("next_counter: {}, smc: {}", next_counter, smc);
                            let mut del_vec: Vec<u64> = Vec::new(); //smpl.iter is immutable borrow,cant remove while looping
                            for &s in smpl.iter() {
                                if s < smc {
                                    del_vec.push(s);
                                    psdl.insert(s);
                                }
                                if s >= smc && nse.load(Relaxed) == 0 {
                                    del_vec.push(s);
                                    println!("1#nse {}",s);
                                    nse.store(s, Relaxed);
                                }
                            }
                            for &s in del_vec.iter() {
                                smpl.remove(&s);
                            }
                        } else {
                            let mut del_vec: Vec<u64> = Vec::new();
                            nse.store(0, Relaxed);
                            for &p in psdl.iter() {
                                if p >= smc {
                                    if nse.load(Relaxed) == 0 {
                                        println!("2#nse {}",p);
                                        nse.store(p, Relaxed);
                                    } else {
                                        smpl.insert(p);
                                    }
                                    del_vec.push(p);
                                }
                            }
                            for &s in del_vec.iter() {
                                psdl.remove(&s);
                            }
                        }
                    }
                }
            }
        });
        // this is a standalone thread for calling callbacks
        //  let counter = Arc::clone(&self.elapsed_samples);
        // TODO  callbacks also based on absolute play time (counter instead of media_counter)
        let media_counter = Arc::clone(&self.media_elapsed_samples);
        let nse = Arc::clone(&self.next_sample_callback);
        let sample_callbacks = Arc::clone(&self.sample_callbacks);
        let stp1 = Arc::clone(&stopped);
        let paused = Arc::clone(&self.paused);
        thread::spawn(move || {
            while !stp1.load(Relaxed) {
                let psd = paused.load(Relaxed);
                if psd {
                    thread::sleep(Duration::from_millis(50));
                    continue;
                }
                let target = nse.load(Relaxed);
                if target != 0 && media_counter.load(Relaxed) >= target {
                    callback_sender.send(CbOnSample(target)).ok();
                    let next = sample_callbacks.lock().unwrap().pop_first();
                    nse.store(next.unwrap_or(0), Relaxed);
                    passed_callbacks.lock().unwrap().insert(next.unwrap_or(0));
                } else {
                    thread::sleep(Duration::from_millis(50));
                }
            }
        });

        let counter = Arc::clone(&self.elapsed_samples);
        let media_counter = Arc::clone(&self.media_elapsed_samples);


        let data_callback = move |out: &mut [f32], _: &cpal::OutputCallbackInfo| {
            // claim the pending seek in a single RMW; a plain load/store pair would let
            // a second seek slip in between and be consumed without clearing the buffer
            let seek_to = psb.swap(NO_SEEK, Relaxed);
            if seek_to != NO_SEEK {
                consumer.clear();
                media_counter.store(seek_to, Relaxed); // resync after the clear, not before
            }
            for sample in out.iter_mut() {
                *sample = consumer.try_pop().unwrap_or(0.0);
            }
            counter.fetch_add(out.len() as u64, Relaxed);
            media_counter.fetch_add(out.len() as u64, Relaxed);
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
        let device_output_info = self.device_output_info;
        callback_shared().set_callback_receiver(
            callback_receiver,
            callback_register_tx,
            device_output_info,
        );
        let stpd = Arc::clone(&stopped);
        let handle = thread::spawn(move || {
            if let Err(e) = streamer
                .play(device_output_info, sender, notifer_tx)
                .join()
                .unwrap_or(Err(StreamErr::UnknownError))
            {
                eprintln!("playback error: {e:?}");
            }
            stpd.store(true, Relaxed);

            // sender dropped here → channel closes → consumer thread exits after drain
        });
        let paused = Arc::clone(&self.paused);

        let stpd = Arc::clone(&stopped);
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
                    stpd.store(true, Relaxed);
                    break;
                }
            }
        });

        Ok(handle)
    }

    pub fn status(&self) -> PlayerStatus {
        PlayerStatus {
            elapsed_samples: Arc::clone(&self.elapsed_samples),
            device_output_info: self.device_output_info,
        }
    }
}

#[derive(Clone)]
pub struct PlayerStatus {
    elapsed_samples: Arc<AtomicU64>,
    device_output_info: DeviceOutputInfo,
}

impl PlayerStatus {
    pub fn get_play_time_ms(&self) -> f32 {
        let elapsed_samples = self.elapsed_samples.load(Relaxed);
        elapsed_samples as f32
            / (self.device_output_info.sample_rate as f32 * self.device_output_info.channels as f32)
            * 1000.0
    }

    pub fn sample_rate(&self) -> u32 {
        self.device_output_info.sample_rate
    }

    pub fn channels(&self) -> u16 {
        self.device_output_info.channels
    }
}
