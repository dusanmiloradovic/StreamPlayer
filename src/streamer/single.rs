use crate::streamer::{Sink, StreamErr, Streamer};
use cpal::default_host;
use cpal::traits::{DeviceTrait, HostTrait};
use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::DecoderOptions;
use symphonia::core::errors::Error;
use symphonia::core::formats::FormatOptions;
use symphonia::core::io::{MediaSource, MediaSourceStream};
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;

struct SingleStreamer {
    media_source: Box<dyn MediaSource>,
    mime_type: String,
    paused: bool,
}

impl SingleStreamer {
    pub fn new(source: Box<dyn MediaSource>, mime_type: String) -> Self {
        Self {
            media_source: source,
            mime_type,
            paused: false,
        }
    }
}

impl Streamer for SingleStreamer {
    fn play(self) -> Result<(), StreamErr> {
        let mut sink = self.get_sink().ok_or(StreamErr::NoSink)?;

        let mss = MediaSourceStream::new(self.media_source, Default::default());
        let mut hint = Hint::new();
        hint.mime_type(&self.mime_type);
        // Use the default options for metadata and format readers.
        let meta_opts: MetadataOptions = Default::default();
        let fmt_opts: FormatOptions = Default::default();

        // Probe the media source.
        let  _probed = symphonia::default::get_probe().format(&hint, mss, &fmt_opts, &meta_opts);
        let Ok(mut probed) = _probed else {
            return Err(StreamErr::UnsupportedFormat);
        };

        if let Some(metadata_rev) = probed.metadata.get().as_ref().and_then(|m| m.current()) {
            for tag in metadata_rev.tags() {
                println!("[probe] {:?} = {}", tag.std_key, tag.value);
            }
        }

        let mut format = probed.format;
        let mut sample_buf = None;

        let _track = format.default_track();
        let Some(track) = _track else{
            return Err(StreamErr::NoAudioTrack);
        };
        let track_id = track.id;

        let dec_opts: DecoderOptions = Default::default();

        let Some(sample_rate) = &track.codec_params.sample_rate else {
            return Err(StreamErr::NoSampleRate);
        };

        let channels = &track.codec_params.channels.unwrap();
        let track_channels_size = channels.count();

        let host = default_host();
        let _device = host
            .default_output_device();
        let Some(device) = _device else{
            return Err(StreamErr::NoOutputDevice)
        };

        let supported = device
            .supported_output_configs()
            .expect("error querying output configs")
            .any(|config| config.channels() == track_channels_size as u16);
        if !supported {
            return Err(StreamErr::UnsupportedChannelCount);
        }
        let mut decoder = symphonia::default::get_codecs()
            .make(&track.codec_params, &dec_opts)
            .expect("unsupported codec");
        loop {
            let packet = match format.next_packet() {
                Ok(packet) => packet,
                Err(Error::ResetRequired) => {
                    return Err(StreamErr::UnknownError);
                }
                Err(Error::IoError(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                    sink.stop();
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
            if packet.track_id() != track_id {
                continue;
            }

            // Decode the packet into audio samples.
            match decoder.decode(&packet) {
                Ok(_decoded) => {
                    if sample_buf.is_none() {
                        let spec = *_decoded.spec();
                        let capacity = _decoded.capacity() as u64; // same as Duration type for SampleBuffer
                        sample_buf = Some(SampleBuffer::<f32>::new(capacity, spec));
                        println!(
                            "Decoded packet with spec: {:?}, capacity: {}",
                            spec, capacity
                        );
                    }
                    if let Some(buf) = &mut sample_buf {
                        buf.copy_interleaved_ref(_decoded);
                        let b = buf.samples();
                        sink.push(b);
                        //TODO interleave first
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
                Err(err) => {
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

    fn stop(self) -> Result<(), StreamErr> {
        todo!()
    }

    fn seek(self, time: u64) -> Result<(), StreamErr> {
        todo!()
    }

    fn get_sink(&self) -> Option<Box<dyn Sink>> {
        todo!()
    }
}
