use audio_learn::streamer::{Sink, Streamer};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{StreamError, SupportedStreamConfig, default_host};
use ringbuf::{HeapProd, HeapRb, traits::*};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, mpsc};

pub trait StreamPlayer {
    fn new(streamer: Box<dyn Streamer>) -> Self;
    fn stop(&mut self);
    fn start(&mut self);
    fn push_samples(&mut self, sample: &[f32]);
}

const CHUNK_SIZE: usize = 1024;

pub struct StreamPlayerImpl {
    default_sample_rate: u32,
    channels: u16,
    stop_channel_sender: Option<mpsc::Sender<()>>,
    producer: Option<HeapProd<f32>>,
    config: SupportedStreamConfig,
}

pub fn new_stream_player(streamer: &dyn Streamer) -> StreamPlayerImpl {
    StreamPlayerImpl::new(streamer)
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

impl StreamPlayerImpl{
    fn new(streamer: &dyn Streamer) -> Self {
        let channels = streamer.get_input_channel_count();
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

         Self {
            channels,
            default_sample_rate,
            stop_channel_sender: None,
            producer: None,
            config,
        }

    }
}



impl Sink for StreamPlayerImpl {
    fn push(&mut self, data: &[f32]) {
        if self.producer.is_none() {
            return;
        }

        push_with_backpressure(self.producer.as_mut().unwrap(), data);
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
        let sample_counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&sample_counter);
        // TODO sample counter to struct, so it can be shared
        // that is where the playing time will be calculated

        let data_callback = move |out: &mut [f32], _: &cpal::OutputCallbackInfo| {
            for sample in out.iter_mut() {
                *sample = consumer.try_pop().unwrap_or(0.0);
                counter_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        };
        let err_fn = |err: StreamError| eprintln!("an error occurred on stream: {err}");
        let host = default_host();
        let device = host
            .default_output_device()
            .expect("no output device found");

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
}
