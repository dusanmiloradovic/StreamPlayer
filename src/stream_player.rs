use audio_learn::streamer::{StreamErr, Streamer};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{default_host, StreamError, SupportedStreamConfig};
use ringbuf::{traits::*, HeapRb};
use std::sync::mpsc;
use std::thread;

pub struct StreamPlayerImpl {
    default_sample_rate: u32,
    channels: u16,
    config: SupportedStreamConfig,
    streamer: Option<Box<dyn Streamer + Send>>,
}

pub fn new_stream_player(streamer: Box<dyn Streamer + Send>) -> Result<StreamPlayerImpl, StreamErr> {
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

impl StreamPlayerImpl {
    fn new(streamer: Box<dyn Streamer + Send>) -> Result<Self, StreamErr> {
        let channels = streamer.get_input_channel_count();
        let input_sample_rate = streamer.get_input_sample_rate();
        let host = default_host();
        let device = host.default_output_device().ok_or(StreamErr::NoOutputDevice)?;

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

        let (stop_tx, stop_rx) = mpsc::channel::<()>();

        // Drains the channel into the ring buffer; after the channel closes, waits
        // for the ring buffer to drain before signalling the cpal keepalive thread.
        thread::spawn(move || {
            while let Ok(samples) = receiver.recv() {
                push_with_backpressure(&mut producer, &samples);
            }
            let drain_time = std::time::Duration::from_secs_f32(ring_size as f32 / sample_rate as f32);
            thread::sleep(drain_time);
            let _ = stop_tx.send(());
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

        // Holds the cpal stream alive until the ring buffer has drained.
        thread::spawn(move || {
            let _stream = stream;
            let _ = stop_rx.recv();
        });

        let mut streamer = self.streamer.take().expect("start() called twice");
        let handle = thread::spawn(move || {
            if let Err(e) = streamer.play(sender) {
                eprintln!("playback error: {e:?}");
            }
            // sender dropped here → channel closes → consumer thread exits after drain
        });

        Ok(handle)
    }
}
