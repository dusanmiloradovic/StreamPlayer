use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{default_host, StreamError};
use ringbuf::{traits::*, HeapProd, HeapRb};
use std::sync::mpsc;

pub trait StreamPlayer {
    fn new(audio_sample_rate: u32, channels: u16) -> Self;
    fn stop(&mut self);
    fn start(&mut self);
    fn push_samples(&mut self, sample: &[f32]);
}

struct StreamPlayerImpl {
    audio_sample_rate: u32,
    default_sample_rate: u32,
    channels: u16,
    stream: Option<cpal::Stream>,
    stop_channel_sender: Option<mpsc::Sender<()>>,
    producer: Option<HeapProd<f32>>,
}

impl StreamPlayer for StreamPlayerImpl {
    fn new(audio_sample_rate: u32, channels: u16) -> Self {
        let host = cpal::default_host();
        let default_device = host.default_output_device().unwrap();
        let default_config = default_device.default_output_config().unwrap();
        Self {
            audio_sample_rate,
            channels,
            default_sample_rate: default_config.sample_rate(),
            stream: None,
            stop_channel_sender: None,
            producer:None,

        }
    }
    fn stop(&mut self) {
        if let Some(stream) = self.stream.take() {
            stream.pause().expect("TODO: panic message");
        }
        if let Some(stop_channel_sender) = self.stop_channel_sender.take() {
            stop_channel_sender.send(()).unwrap();
        }
        self.stream = None;
        self.stop_channel_sender = None;
    }
    fn start(&mut self) {
        let (producer, mut consumer) = HeapRb::<f32>::new(1024).split();
        self.producer = Some(producer);
        let data_callback = move |out: &mut [f32], _: &cpal::OutputCallbackInfo| {
            for sample in out.iter_mut() {
                if consumer.is_empty() {
                    *sample = 0.0f32;
                } else {
                    if let Some(s) = consumer.try_pop() {
                        *sample = s;
                    } else {
                        *sample = 0.0f32;
                    }
                }
            }
        };
        let err_fn = |err: StreamError| eprintln!("an error occurred on stream: {err}");
        let host = default_host();
        let device = host
            .default_output_device()
            .expect("no output device found");
        let config = device
            .default_output_config()
            .expect("no default output config");
        let stream = device
            .build_output_stream(&config.into(), data_callback, err_fn, None)
            .expect("failed to build stream");
        stream.play().expect("failed to start stream");
        self.stream = Some(stream);
        let (stop_channel_sender, stop_channel_receiver) = mpsc::channel::<()>();
        self.stop_channel_sender = Some(stop_channel_sender);
        stop_channel_receiver.recv().unwrap();
    }

    fn push_samples(&mut self, sample: &[f32]) {
        // TODO samples rate adjust with Rubato
        if let Some(producer) = self.producer.as_mut() {
            producer.push_slice(sample);
        }
    }
}
