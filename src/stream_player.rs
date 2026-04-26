use audioadapter_buffers::direct::InterleavedSlice;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{default_host, StreamError, SupportedStreamConfig};
use ringbuf::{traits::*, HeapProd, HeapRb};
use rubato::{Fft, FixedSync, Resampler};
use std::sync::mpsc;

pub trait StreamPlayer {
    fn new(audio_sample_rate: u32, channels: u16) -> Self;
    fn stop(&mut self);
    fn start(&mut self);
    fn push_samples(&mut self, sample: &[f32]);
}

const CHUNK_SIZE: usize = 1024;

pub struct StreamPlayerImpl {
    audio_sample_rate: u32,
    default_sample_rate: u32,
    channels: u16,
    stop_channel_sender: Option<mpsc::Sender<()>>,
    producer: Option<HeapProd<f32>>,
    resampler: Option<Fft<f32>>,
    input_buffer: Vec<f32>,
    config:SupportedStreamConfig,
}

pub fn new_stream_player(audio_sample_rate: u32, channels: u16) -> StreamPlayerImpl {
    StreamPlayerImpl::new(audio_sample_rate, channels)
}

fn push_with_backpressure(producer: &mut HeapProd<f32>, data: &[f32]) {
    let mut offset = 0;
    while offset < data.len() {
        let pushed = producer.push_slice(&data[offset..]);
        offset += pushed;
        if offset < data.len() {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
}

impl StreamPlayer for StreamPlayerImpl {
    fn new(audio_sample_rate: u32, channels: u16) -> Self {
        let host = default_host();
        let default_device = host.default_output_device().unwrap();
        // let default_config = default_device.default_output_config().unwrap();
        // let default_sample_rate = default_config.sample_rate();

        let config = default_device
            .supported_output_configs()
            .expect("failed to query output configs")
            .find(|c| c.channels() == channels)
            .expect("no output config found for the requested channel count")
            .with_max_sample_rate();
        let default_sample_rate = config.sample_rate();

        let resampler = if audio_sample_rate != default_sample_rate {
            Some(
                Fft::<f32>::new(
                    audio_sample_rate as usize,
                    default_sample_rate as usize,
                    CHUNK_SIZE,
                    2,
                    channels as usize,
                    FixedSync::Input,
                )
                .expect("failed to create resampler"),
            )
        } else {
            None
        };

        Self {
            audio_sample_rate,
            channels,
            default_sample_rate,
            stop_channel_sender: None,
            producer: None,
            resampler,
            input_buffer: Vec::new(),
            config,
        }
    }

    fn stop(&mut self) {
        if let Some(sender) = self.stop_channel_sender.take() {
            let _ = sender.send(());
        }
    }

    fn start(&mut self) {
        let target_latency_secs = 1f32;
        let raw_size =
            (self.default_sample_rate as f32 * self.channels as f32 * target_latency_secs) as usize;
        let ring_size = raw_size.next_power_of_two();
        let (producer, mut consumer) = HeapRb::<f32>::new(ring_size).split();
        self.producer = Some(producer);

        let data_callback = move |out: &mut [f32], _: &cpal::OutputCallbackInfo| {
            for sample in out.iter_mut() {
                *sample = consumer.try_pop().unwrap_or(0.0);
            }
        };
        let err_fn = |err: StreamError| eprintln!("an error occurred on stream: {err}");
        let host = default_host();
        let device = host.default_output_device().expect("no output device found");

        let stream = device
            .build_output_stream(&self.config.config(), data_callback, err_fn, None)
            .expect("failed to build stream");
        stream.play().expect("failed to start stream");

        let (stop_tx, stop_rx) = mpsc::channel::<()>();
        self.stop_channel_sender = Some(stop_tx);
        std::thread::spawn(move || {
            let _stream = stream;
            let _ = stop_rx.recv();
        });
    }

    fn push_samples(&mut self, samples: &[f32]) {
        if self.producer.is_none() {
            return;
        }

        if self.resampler.is_some() {
            self.input_buffer.extend_from_slice(samples);

            let ch = self.channels as usize;
            let samples_per_chunk = CHUNK_SIZE * ch;

            while self.input_buffer.len() >= samples_per_chunk {
                let max_out_frames = self.resampler.as_ref().unwrap().output_frames_max();
                let mut outdata = vec![0.0f32; max_out_frames * ch];

                let actual_out_frames = {
                    let chunk = &self.input_buffer[..samples_per_chunk];
                    let input_adapter = InterleavedSlice::new(chunk, ch, CHUNK_SIZE)
                        .expect("failed to create input adapter");
                    let mut output_adapter =
                        InterleavedSlice::new_mut(&mut outdata, ch, max_out_frames)
                            .expect("failed to create output adapter");
                    self.resampler
                        .as_mut()
                        .unwrap()
                        .process_into_buffer(&input_adapter, &mut output_adapter, None)
                        .expect("resampling failed")
                        .1
                };

                let resampled = &outdata[..actual_out_frames * ch];
                push_with_backpressure(self.producer.as_mut().unwrap(), resampled);
                self.input_buffer.drain(..samples_per_chunk);
            }
        } else {
            push_with_backpressure(self.producer.as_mut().unwrap(), samples);
        }
    }
}
