use audio_learn::streamer::{Sink, StreamErr, Streamer};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{default_host, StreamError, SupportedStreamConfig};
use ringbuf::{traits::*, HeapProd, HeapRb};
use std::sync::atomic::AtomicUsize;
use std::sync::{mpsc, Arc};

pub trait StreamPlayer {
    fn new(streamer: Box<dyn Streamer>) -> Self;
    fn stop(&mut self);
    fn start(&mut self) -> Result<(), StreamErr>;
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

pub fn new_stream_player(streamer: &dyn Streamer) -> Result<StreamPlayerImpl,StreamErr> {
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

impl StreamPlayerImpl {
    fn new(streamer: &dyn Streamer) -> Result<Self, StreamErr> {
        let channels = streamer.get_input_channel_count();
        let track_channels_size = streamer.get_input_channel_count();
        let input_sample_rate = streamer.get_input_sample_rate();
        let host = default_host();
        let default_device = host.default_output_device().unwrap();
        // let default_config = default_device.default_output_config().unwrap();
        // let default_sample_rate = default_config.sample_rate();

        let config_range = default_device
            .supported_output_configs()
            .map_err(|_| StreamErr::QueryOutputDeviceError)?
            .find(|c| c.channels() == channels)
            .ok_or_else(|| StreamErr::NoDeviceConfigForChannelCount)?;

        let closest = input_sample_rate.clamp(
            config_range.min_sample_rate(),
            config_range.max_sample_rate(),
        );
        let config = config_range.with_sample_rate(closest);
        let default_sample_rate = config.sample_rate();

        Ok(Self {
            channels,
            default_sample_rate,
            stop_channel_sender: None,
            producer: None,
            config,
        })
    }
}

impl Sink for StreamPlayerImpl {
    fn push(&mut self, data: &[f32]) -> Result<(), StreamErr>{
        if self.producer.is_none() {
            return Ok(());
        }

        push_with_backpressure(self.producer.as_mut().unwrap(), data);
        Ok(())
    }

    fn stop(&mut self) -> Result<(), StreamErr>{
        if let Some(sender) = self.stop_channel_sender.take() {
            let _ = sender.send(());
        }
        Ok(())
    }

    fn start(&mut self) -> Result<(), StreamErr>{
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
            .default_output_device().ok_or_else(|| StreamErr::NoDeviceConfigForChannelCount)?;


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
        Ok(())
    }
}
