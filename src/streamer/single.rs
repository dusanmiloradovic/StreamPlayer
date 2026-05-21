use crate::streamer::{Sink, StreamErr, Streamer};
use audioadapter_buffers::direct::InterleavedSlice;
use cpal::traits::{DeviceTrait, HostTrait};
use cpal::default_host;
use rubato::{Fft, FixedSync, Resampler};
use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::{CodecParameters, DecoderOptions};
use symphonia::core::errors::Error;
use symphonia::core::formats::FormatOptions;
use symphonia::core::io::{MediaSource, MediaSourceStream};
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::{Hint, ProbeResult};

const CHUNK_SIZE: usize = 1024;

pub struct SingleStreamer {
    mime_type: String,
    paused: bool,
    resampler: Option<Fft<f32>>,
    resampling_buffer: Vec<f32>,
    sink: Option<Box<dyn Sink>>,
    default_sample_rate: u32,
    input_sample_rate: u32,
    probe_result: ProbeResult,
    channels_size: u16,
    track_id: u32,
    codec_params: CodecParameters,
}

// had to move out of the struct because of borrow checker(if this was a method, self would be borrowed)
fn resample(
    resampler: &mut Option<Fft<f32>>,
    resampling_buffer: &mut Vec<f32>,
    sink: &mut Option<Box<dyn Sink>>,
    input_channels: u16,
    samples: &[f32],
) -> Result<(), StreamErr> {
    if let Some(r) = resampler {
        resampling_buffer.extend_from_slice(samples);
        let samples_per_chunk = CHUNK_SIZE * input_channels as usize;
        // Only process complete chunks; leave any remainder in the buffer for the next call.
        // Zero-padding a partial chunk mid-stream corrupts the FFT resampler's internal state.
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
            sink.as_mut().ok_or(StreamErr::NoSink)?.push(resampled)?;
            resampling_buffer.drain(..samples_per_chunk);
        }
        Ok(())
    } else {
        sink.as_mut().ok_or(StreamErr::NoSink)?.push(samples)?;
        Ok(())
    }
}

impl SingleStreamer {
    pub fn new(source: Box<dyn MediaSource>, mime_type: String) -> Result<Self, StreamErr> {
        let mss = MediaSourceStream::new(source, Default::default());
        let mut hint = Hint::new();
        hint.mime_type(&mime_type);
        // Use the default options for metadata and format readers.
        let meta_opts: MetadataOptions = Default::default();
        let fmt_opts: FormatOptions = Default::default();

        // Probe the media source.
        let _probed = symphonia::default::get_probe().format(&hint, mss, &fmt_opts, &meta_opts);
        let Ok(probed) = _probed else {
            return Err(StreamErr::UnsupportedFormat);
        };

        // if let Some(metadata_rev) = probed.metadata.get().as_ref().and_then(|m| m.current()) {
        //     for tag in metadata_rev.tags() {
        //         println!("[probe] {:?} = {}", tag.std_key, tag.value);
        //     }
        // }

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
        let _device = host.default_output_device();
        let Some(device) = _device else {
            return Err(StreamErr::NoOutputDevice);
        };

        // let config = device
        //     .supported_output_configs()
        //     .map_err(|_| StreamErr::QueryOutputDeviceError)?
        //     .find(|c| c.channels() == track_channels_size)
        //     .ok_or_else(|| StreamErr::NoDeviceConfigForChannelCount)?
        //     .with_max_sample_rate();
        let config_range = device
            .supported_output_configs()
            .map_err(|_| StreamErr::QueryOutputDeviceError)?
            .find(|c| c.channels() == track_channels_size)
            .ok_or_else(|| StreamErr::NoDeviceConfigForChannelCount)?;

        let closest = sample_rate.clamp(
            config_range.min_sample_rate(),
            config_range.max_sample_rate(),
        );
        let config = config_range.with_sample_rate(closest);
        let config_sample_rate = config.sample_rate();
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
            mime_type,
            paused: false,
            resampler,
            resampling_buffer: Vec::new(),
            sink: None,
            default_sample_rate: 0,
            input_sample_rate: sample_rate,
            probe_result: probed,
            channels_size: track_channels_size,
            track_id,
            codec_params,
        })
    }
}

impl Streamer for SingleStreamer {
    fn play(&mut self) -> Result<(), StreamErr> {
        if self.sink.is_none() {
            return Err(StreamErr::NoSink);
        }
        let mut sample_buf = None;
        let dec_opts: DecoderOptions = Default::default();
        let mut decoder = symphonia::default::get_codecs()
            .make(&self.codec_params, &dec_opts)
            .or_else(|_| Err(StreamErr::UnsupportedCodec))?;
        let format = self.probe_result.format.as_mut();
        loop {
            let packet = match format.next_packet() {
                Ok(packet) => packet,
                Err(Error::ResetRequired) => {
                    return Err(StreamErr::UnknownError);
                }
                Err(Error::IoError(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                    self.sink.as_mut().unwrap().stop()?;
                    break;
                }
                Err(err) => {
                    // A unrecoverable error occurred, halt decoding.
                    eprintln!("Packet read error debug: {:#?}", err);
                    return Err(StreamErr::UnknownError);
                }
            };

            // Consume any new metadata that has been read since the last packet.
            while !format.metadata().is_latest() {
                // Pop the old head of the metadata queue.
                format.metadata().pop();

                // Consume the new metadata at the head of the metadata queue.
            }

            // If the packet does not belong to the selected track, skip over it.
            if packet.track_id() != self.track_id {
                continue;
            }

            // Decode the packet into audio samples.
            match decoder.decode(&packet) {
                Ok(_decoded) => {
                    if sample_buf.is_none() {
                        let spec = *_decoded.spec();
                        let capacity = _decoded.capacity() as u64; // same as Duration type for SampleBuffer
                        sample_buf = Some(SampleBuffer::<f32>::new(capacity, spec));
                    }
                    if let Some(buf) = &mut sample_buf {
                        buf.copy_interleaved_ref(_decoded);
                        resample(
                            &mut self.resampler,
                            &mut self.resampling_buffer,
                            &mut self.sink,
                            self.channels_size,
                            buf.samples(),
                        )?;
                    }
                }
                Err(Error::IoError(_)) => {
                    // The packet failed to decode due to an IO error, skip the packet.
                    continue;
                }
                Err(Error::DecodeError(_)) => {
                    // The packet failed to decode due to invalid data, skip the packet.
                    continue;
                }
                Err(_) => {
                    return Err(StreamErr::UnknownError);
                }
            }
        }
        Ok(())
    }

    fn pause(&mut self) -> Result<(), StreamErr> {
        self.paused = true;
        Ok(())
    }

    fn stop(&self) -> Result<(), StreamErr> {
        todo!()
    }

    fn seek(&self, time: u64) -> Result<(), StreamErr> {
        todo!()
    }

    fn set_sink(&mut self, sink: Box<dyn Sink>) {
        self.sink = Some(sink);
    }

    fn get_input_sample_rate(&self) -> u32 {
        self.input_sample_rate
    }

    fn get_input_channel_count(&self) -> u16 {
        self.channels_size
    }
}
