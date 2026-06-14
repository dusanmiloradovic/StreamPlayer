use std::collections::HashMap;
use crate::streamer::{Callback, StreamErr, Streamer};
use audioadapter_buffers::direct::InterleavedSlice;
use cpal::default_host;
use cpal::traits::{DeviceTrait, HostTrait};
use rubato::{Fft, FixedSync, Resampler};
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::{Arc, mpsc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::{CodecParameters, DecoderOptions};
use symphonia::core::errors::Error;
use symphonia::core::formats::{FormatOptions, SeekMode, SeekTo};

use symphonia::core::io::{MediaSource, MediaSourceStream};
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::{Hint, ProbeResult};
use symphonia::core::units::Time;
use crate::streamer::utils::execute_callback;

const CHUNK_SIZE: usize = 1024;

enum StreamerCommand {
    Seek(SeekTo),
    Stop,
    Rewind,
}

pub struct SingleStreamer {
    paused: Arc<AtomicBool>,
    resampler: Option<Fft<f32>>,
    resampling_buffer: Vec<f32>,
    input_sample_rate: u32,
    probe_result: Option<ProbeResult>,
    channels_size: u16,
    track_id: u32,
    codec_params: CodecParameters,
    output_sample_rate: u32,
    finished: Arc<AtomicBool>,
    command_tx: Option<mpsc::SyncSender<StreamerCommand>>,
    callbacks: Arc<Mutex<HashMap<u64, Box<dyn Fn() + Send>>>>,
    callback_register: Option<SyncSender<u64>>,
}

// Free function to avoid borrow conflict between self.probe_result.format and other fields.
fn resample(
    resampler: &mut Option<Fft<f32>>,
    resampling_buffer: &mut Vec<f32>,
    sender: &SyncSender<Vec<f32>>,
    input_channels: u16,
    samples: &[f32],
) -> Result<(), StreamErr> {
    if let Some(r) = resampler {
        resampling_buffer.extend_from_slice(samples);
        let samples_per_chunk = CHUNK_SIZE * input_channels as usize;
        let max_out_frames = r.output_frames_max();
        let mut outdata = vec![0.0f32; max_out_frames * input_channels as usize];
        while resampling_buffer.len() >= samples_per_chunk {
            let actual_out_frames = {
                let chunk = &resampling_buffer[..samples_per_chunk];
                let input_adapter =
                    InterleavedSlice::new(chunk, input_channels as usize, CHUNK_SIZE)
                        .or_else(|_| Err(StreamErr::ResamplingError))?;
                let mut output_adapter = InterleavedSlice::new_mut(
                    &mut outdata,
                    input_channels as usize,
                    max_out_frames,
                )
                .or_else(|_| Err(StreamErr::ResamplingError))?;
                r.process_into_buffer(&input_adapter, &mut output_adapter, None)
                    .or_else(|_| Err(StreamErr::ResamplingError))?
                    .1
            };
            let resampled = &outdata[..actual_out_frames * input_channels as usize];
            sender
                .send(resampled.to_vec())
                .map_err(|_| StreamErr::SendError)?;
            resampling_buffer.drain(..samples_per_chunk);
        }
        Ok(())
    } else {
        sender
            .send(samples.to_vec())
            .map_err(|_| StreamErr::SendError)?;
        Ok(())
    }
}

impl SingleStreamer {
    pub fn new(source: Box<dyn MediaSource>, mime_type: String) -> Result<Self, StreamErr> {
        let mss = MediaSourceStream::new(source, Default::default());
        let mut hint = Hint::new();
        hint.mime_type(&mime_type);
        let meta_opts: MetadataOptions = Default::default();
        let fmt_opts: FormatOptions = Default::default();

        let probed = symphonia::default::get_probe()
            .format(&hint, mss, &fmt_opts, &meta_opts)
            .map_err(|_| StreamErr::UnsupportedFormat)?;

        let (track_id, sample_rate, track_channels_size, codec_params) = {
            let track = probed
                .format
                .default_track()
                .ok_or(StreamErr::NoAudioTrack)?;
            let id = track.id;
            let sr = track
                .codec_params
                .sample_rate
                .ok_or(StreamErr::NoSampleRate)?;
            let ch = track.codec_params.channels.unwrap().count() as u16;
            let cp = track.codec_params.clone();
            (id, sr, ch, cp)
        };

        let host = default_host();
        let device = host
            .default_output_device()
            .ok_or(StreamErr::NoOutputDevice)?;

        let config_range = device
            .supported_output_configs()
            .map_err(|_| StreamErr::QueryOutputDeviceError)?
            .find(|c| c.channels() == track_channels_size)
            .ok_or(StreamErr::NoDeviceConfigForChannelCount)?;

        let closest = sample_rate.clamp(
            config_range.min_sample_rate(),
            config_range.max_sample_rate(),
        );
        let config_sample_rate = config_range.with_sample_rate(closest).sample_rate();

        let resampler = if config_sample_rate != sample_rate {
            Fft::<f32>::new(
                sample_rate as usize,
                config_sample_rate as usize,
                CHUNK_SIZE,
                2,
                track_channels_size as usize,
                FixedSync::Input,
            )
            .ok()
        } else {
            None
        };

        Ok(Self {
            paused: Arc::new(AtomicBool::new(false)),
            resampler,
            resampling_buffer: Vec::new(),
            input_sample_rate: sample_rate,
            probe_result: Some(probed),
            channels_size: track_channels_size,
            track_id,
            codec_params,
            output_sample_rate: config_sample_rate,
            finished: Arc::new(AtomicBool::new(false)),
            command_tx: None,
            callbacks: Arc::new(Mutex::new(HashMap::new())),
            callback_register: None,
        })
    }
}

impl Streamer for SingleStreamer {
    fn play(&mut self, sender: SyncSender<Vec<f32>>, callback_receiver: Receiver<Callback>, callback_register: SyncSender<u64>) -> JoinHandle<Result<(), StreamErr>> {
        let codec_params = self.codec_params.clone();
        let track_id = self.track_id;
        let channels_size = self.channels_size;
        let format = self.probe_result.take().map(|p| p.format);
        let resampler = if self.output_sample_rate != self.input_sample_rate {
            Fft::<f32>::new(
                self.input_sample_rate as usize,
                self.output_sample_rate as usize,
                CHUNK_SIZE,
                2,
                self.channels_size as usize,
                FixedSync::Input,
            )
            .ok()
        } else {
            None
        };

        let (cmd_tx, cmd_rx) = mpsc::sync_channel::<StreamerCommand>(4);
        self.command_tx = Some(cmd_tx);

        let finished = self.finished.clone();
        let paused = self.paused.clone();

        let callbacks = self.callbacks.clone();
        thread::spawn(move || -> Result<(), StreamErr> {
            let mut format = format.ok_or(StreamErr::AlreadyPlaying)?;
            let mut resampler = resampler;
            let mut resampling_buffer: Vec<f32> = Vec::new();
            let mut sample_buf = None;
            let dec_opts: DecoderOptions = Default::default();
            let mut decoder = symphonia::default::get_codecs()
                .make(&codec_params, &dec_opts)
                .map_err(|_| StreamErr::UnsupportedCodec)?;
            loop {
                while paused.load(std::sync::atomic::Ordering::Acquire) {
                    thread::sleep(std::time::Duration::from_millis(10));
                }
                match cmd_rx.try_recv() {
                    Ok(StreamerCommand::Seek(to)) => {
                        format.seek(SeekMode::Accurate, to).ok();
                        decoder.reset(); // mandatory after seek
                    }
                    Ok(StreamerCommand::Stop) => {
                        finished.store(true, std::sync::atomic::Ordering::Relaxed);
                        return Ok(());
                    }
                    Ok(StreamerCommand::Rewind) => {
                        format
                            .seek(SeekMode::Accurate, SeekTo::TimeStamp { ts: 0, track_id })
                            .ok();
                        decoder.reset();
                    }
                    Err(_) => {} // nothing pending, continue
                }
                match callback_receiver.try_recv(){
                    Ok(Callback::Callback(callback_time)) => {
                       execute_callback(&callbacks,callback_time);
                    }
                    Err(_) => {}
                }

                let packet = match format.next_packet() {
                    Ok(packet) => packet,
                    Err(Error::ResetRequired) => {
                        finished.store(true, std::sync::atomic::Ordering::Relaxed);
                        return Err(StreamErr::UnknownError);
                    }
                    Err(Error::IoError(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                        finished.store(true, std::sync::atomic::Ordering::Relaxed);
                        return Ok(());
                    }
                    Err(err) => {
                        eprintln!("packet read error: {err:#?}");
                        finished.store(true, std::sync::atomic::Ordering::Relaxed);
                        return Err(StreamErr::UnknownError);
                    }
                };

                while !format.metadata().is_latest() {
                    format.metadata().pop();
                }

                if packet.track_id() != track_id {
                    continue;
                }

                match decoder.decode(&packet) {
                    Ok(_decoded) => {
                        if sample_buf.is_none() {
                            let spec = *_decoded.spec();
                            let capacity = _decoded.capacity() as u64;
                            sample_buf = Some(SampleBuffer::<f32>::new(capacity, spec));
                        }
                        if let Some(buf) = &mut sample_buf {
                            buf.copy_interleaved_ref(_decoded);
                            resample(
                                &mut resampler,
                                &mut resampling_buffer,
                                &sender,
                                channels_size,
                                buf.samples(),
                            )?;
                        }
                    }
                    Err(Error::IoError(_)) | Err(Error::DecodeError(_)) => continue,
                    Err(_) => return Err(StreamErr::UnknownError),
                }
            }
        })
    }

    fn pause(&mut self) -> Result<(), StreamErr> {
        let paused = self.paused.clone();
        paused.store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn resume(&mut self) -> Result<(), StreamErr> {
        let paused = self.paused.clone();
        paused.store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn stop(&self) -> Result<(), StreamErr> {
        let tx = self.command_tx.as_ref().ok_or(StreamErr::NotPlaying)?;
        tx.send(StreamerCommand::Stop)
            .map_err(|_| StreamErr::SendError)
    }

    fn seek(&self, _time: u64) -> Result<(), StreamErr> {
        let tx = self.command_tx.as_ref().ok_or(StreamErr::NotPlaying)?;
        let to = SeekTo::Time {
            time: Time::from(_time),
            track_id: None,
        };
        tx.send(StreamerCommand::Seek(to))
            .map_err(|_| StreamErr::SendError)
    }

    fn rewind(&self) -> Result<(), StreamErr> {
        let tx = self.command_tx.as_ref().ok_or(StreamErr::NotPlaying)?;
        tx.send(StreamerCommand::Rewind)
            .map_err(|_| StreamErr::SendError)
    }

    fn get_input_sample_rate(&self) -> u32 {
        self.input_sample_rate
    }

    fn get_input_channel_count(&self) -> u16 {
        self.channels_size
    }

    fn get_output_sample_rate(&self) -> u32 {
        self.output_sample_rate
    }

    fn finished_flag(&self) -> Arc<AtomicBool> {
        self.finished.clone()
    }

    fn add_callback(&mut self, callback_time: u64, callback: Box<dyn Fn() + Send>) {
        self.callback_register.as_ref().unwrap().send(callback_time).unwrap();
        self.callbacks.lock().unwrap().insert(callback_time, callback);
    }

}
