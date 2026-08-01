use crate::streamer::{
    ControlCommand, ControlHandle, DeviceOutputInfo, StreamErr, Streamer, StreamerInputInfo,
};
use audioadapter_buffers::direct::InterleavedSlice;
use rubato::{Fft, FixedSync, Resampler};
use std::borrow::Cow;
use std::fs::File;
use std::io::ErrorKind::UnexpectedEof;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::SyncSender;
use std::thread;
use std::thread::JoinHandle;
use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::DecoderOptions;
use symphonia::core::errors::Error;
use symphonia::core::formats::{FormatOptions, SeekMode, SeekTo};

use symphonia::core::io::{MediaSource, MediaSourceStream};
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::{Hint, ProbeResult};
use symphonia::core::units::Time;

const CHUNK_SIZE: usize = 1024;

pub trait MediaSourceFactory: Send + Sync {
    fn open(&self) -> std::io::Result<Box<dyn MediaSource>>;
}

// if we pass directly the media source, it will keep the file descriptor open
// and that will be a problemd for playlist
pub enum StreamerSource {
    File(PathBuf),
    Http(String, Vec<(String, String)>),
    Custom(Arc<dyn MediaSourceFactory>),
}

fn get_media_source_from_stream_source(s: &StreamerSource) -> Box<dyn MediaSource> {
    if let StreamerSource::File(path) = s {
        Box::new(File::open(path).unwrap())
    } else if let StreamerSource::Http(url, headers) = s {
        panic!("Not implemented");
    } else if let StreamerSource::Custom(factory) = s {
        factory.open().unwrap()
    } else {
        panic!("Invalid streamer source");
    }
}

pub struct SingleStreamer {
    streamer_source: StreamerSource,
    mime_type: String,
    finished: Arc<AtomicBool>,
    control: ControlHandle,
    command_rx: Option<crossbeam_channel::Receiver<ControlCommand>>,
    streamer_input_info: Option<StreamerInputInfo>,
    output_info: Option<DeviceOutputInfo>,
}

// Free function to avoid borrow conflict between self.probe_result.format and other fields.
fn resample(
    resampler: &mut Option<Fft<f32>>,
    resampling_buffer: &mut Vec<f32>,
    sender: &SyncSender<Vec<f32>>,
    input_channels: u16,
    samples: &[f32],
    resampled_so_far: usize, // the gain function depends on the time (that is the number of samples already resampled)
    gain_function: &Option<Arc<dyn Fn(usize) -> f32 + Send>>,
) -> Result<usize, StreamErr> {
    let mut cnt = resampled_so_far;
    if let Some(r) = resampler {
        resampling_buffer.extend_from_slice(samples);
        let samples_per_chunk = CHUNK_SIZE * input_channels as usize;
        let max_out_frames = r.output_frames_max();
        let mut outdata = vec![0.0f32; max_out_frames * input_channels as usize];
        let mut resampled_len = 0;

        while resampling_buffer.len() >= samples_per_chunk {
            let actual_out_frames = {
                let chunk = &resampling_buffer[..samples_per_chunk];
                let input_adapter =
                    InterleavedSlice::new(chunk, input_channels as usize, CHUNK_SIZE)
                        .map_err(|_| StreamErr::ResamplingError)?;
                let mut output_adapter = InterleavedSlice::new_mut(
                    &mut outdata,
                    input_channels as usize,
                    max_out_frames,
                )
                .map_err(|_| StreamErr::ResamplingError)?;
                r.process_into_buffer(&input_adapter, &mut output_adapter, None)
                    .map_err(|_| StreamErr::ResamplingError)?
                    .1
            };
            let resampled = &mut outdata[..actual_out_frames * input_channels as usize];
            if let Some(gf) = gain_function {
                for j in 0..resampled.len() {
                    let g = gf(cnt + j);
                    resampled[j] *= g;
                }
            }
            resampled_len += resampled.len();
            cnt += resampled.len();
            sender
                .send(resampled.to_vec())
                .map_err(|_| StreamErr::SendError)?;
            resampling_buffer.drain(..samples_per_chunk);
        }
        Ok(resampled_len)
    } else {
        let mut samples_copy = samples.to_vec();
        if let Some(gf) = gain_function {
            for j in 0..samples_copy.len() {
                let g = gf(cnt + j);
                samples_copy[j] *= g;
            }
        }
        sender
            .send(samples_copy)
            .map_err(|_| StreamErr::SendError)?;
        Ok(samples.len())
    }
}

impl SingleStreamer {
    pub fn new(streamer_source: StreamerSource, mime_type: String) -> Result<Self, StreamErr> {
        let (control, command_rx) = ControlHandle::new();
        Ok(Self {
            streamer_source,
            mime_type,
            finished: Arc::new(AtomicBool::new(false)),
            control,
            command_rx: Some(command_rx),
            streamer_input_info: None,
            output_info: None,
        })
    }

    fn get_probe(&self) -> Result<(ProbeResult,bool), StreamErr> {
        let ms = get_media_source_from_stream_source(&self.streamer_source);
        let mss = MediaSourceStream::new(ms, Default::default());
        let seekable = mss.is_seekable();
        let mut hint = Hint::new();
        hint.mime_type(&self.mime_type);
        let meta_opts: MetadataOptions = Default::default();
        let fmt_opts: FormatOptions = Default::default();

        let probed = symphonia::default::get_probe()
            .format(&hint, mss, &fmt_opts, &meta_opts)
            .map_err(|_| StreamErr::UnsupportedFormat)?;
        Ok((probed,seekable))
    }

    fn get_input_info_from_probe(probed: &ProbeResult) -> Result<StreamerInputInfo, StreamErr> {
        let (track_id, sample_rate, channels, codec_params) = {
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
        let mut duration = None;
        if let (Some(tb), Some(n_frames)) = (codec_params.time_base, codec_params.n_frames) {
            let t = tb.calc_time(n_frames);
            duration = Some(t.seconds + t.frac.round() as u64);
        }
        Ok(StreamerInputInfo {
            track_id,
            channels,
            sample_rate,
            duration,
            codec_params,
        })
    }
}

impl Streamer for SingleStreamer {
    fn play(
        &mut self,
        output_info: DeviceOutputInfo,
        sender: SyncSender<Vec<f32>>,
    ) -> JoinHandle<Result<(), StreamErr>> {
        self.output_info = Some(output_info.clone());
        let _probed = self.get_probe();
        if _probed.is_err() {
            return thread::spawn(move || {
                return Err(StreamErr::ProbeError);
            });
        }
        let (probed,seekable) = _probed.unwrap();
        let _input_info = Self::get_input_info_from_probe(&probed);
        let mut format = probed.format;
        if _input_info.is_err() {
            return thread::spawn(move || {
                return Err(StreamErr::InputInfoError);
            });
        }
        let ii = _input_info.unwrap();
        //TODO !!!compare also the channel count, and  interleave or drop channels if required
        let codec_params = ii.codec_params;
        let track_id = ii.track_id;
        let channels_size = ii.channels;

        let resampler = if output_info.sample_rate != ii.sample_rate {
            Fft::<f32>::new(
                ii.sample_rate as usize,
                output_info.sample_rate as usize,
                CHUNK_SIZE,
                2,
                output_info.channels as usize,
                FixedSync::Input,
            )
            .ok()
        } else {
            None
        };

        let cmd_rx = self.command_rx.take();

        let finished = self.finished.clone();
        let paused = self.control.paused_flag();
        let duration = self.get_duration();


        thread::spawn(move || -> Result<(), StreamErr> {
            let cmd_rx = cmd_rx.ok_or(StreamErr::AlreadyPlaying)?;
            //let mut format = format.ok_or(StreamErr::AlreadyPlaying)?;
            let mut resampler = resampler;
            let mut resampling_buffer: Vec<f32> = Vec::new();
            let mut sample_buf = None;
            let dec_opts: DecoderOptions = Default::default();
            let mut decoder = symphonia::default::get_codecs()
                .make(&codec_params, &dec_opts)
                .map_err(|_| StreamErr::UnsupportedCodec)?;
            let mut gain_function: Option<Arc<dyn Fn(usize) -> f32 + Send>> = None;
            let mut resampled_len: usize = 0;

            loop {
                while paused.load(std::sync::atomic::Ordering::Acquire) {
                    thread::sleep(std::time::Duration::from_millis(10));
                }
                match cmd_rx.try_recv() {
                    Ok(ControlCommand::Seek(time)) => {
                        if duration.is_none() || !seekable {
                            // TODO current version will support seek only on seekable streams, revisit later
                            return Err(StreamErr::SeekError);
                        }
                        let duration_ts = duration.unwrap();
                        let target = time.min(duration_ts.saturating_sub(1));
                        let to = SeekTo::Time {
                            time: Time::from(target),
                            track_id: Some(track_id),
                        };
                        match format.seek(SeekMode::Coarse, to) {
                            Ok(s) => {
                                decoder.reset();
                                resampling_buffer.clear();
                                resampled_len = 0;
                                //TODO notify streamer of position
                            } // fine
                            Err(Error::SeekError(_)) => {
                                decoder.reset();
                                resampling_buffer.clear();
                                resampled_len = 0;
                                /* position unknown → re-seek if needed */
                                // TODO get position first somehow (next_packet), and notify streamer
                            }
                            Err(Error::IoError(e)) if e.kind() == UnexpectedEof => {
                                finished.store(true, std::sync::atomic::Ordering::Relaxed);
                                return Ok(());
                            }
                            Err(Error::ResetRequired) => {
                                decoder.reset(); /* then continue */
                                //TODO get position
                            }
                            Err(Error::IoError(_)) => {
                                /* real source failure → source is dead, bail/retry */
                                finished.store(true, std::sync::atomic::Ordering::Relaxed);
                                return Err(StreamErr::SeekError);
                            }
                            Err(_) => {
                                /* Unsupported/Limit — handle */
                                finished.store(true, std::sync::atomic::Ordering::Relaxed);
                                return Err(StreamErr::SeekError);
                            }
                        }
                    }
                    Ok(ControlCommand::Stop) => {
                        finished.store(true, std::sync::atomic::Ordering::Relaxed);
                        return Ok(());
                    }
                    Ok(ControlCommand::Rewind) => {
                        format
                            .seek(SeekMode::Coarse, SeekTo::TimeStamp { ts: 0, track_id })
                            .ok();
                        decoder.reset();
                    }
                    Ok(ControlCommand::AddGainFunction(gf)) => {
                        gain_function = Some(gf);
                        resampled_len = 0; // gain functions starts from the sample 0
                    }
                    Ok(ControlCommand::RemoveGainFunction) => {
                        gain_function = None;
                        resampled_len = 0;
                    } // nothing pending, continue
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
                            if let Ok(rl) = resample(
                                &mut resampler,
                                &mut resampling_buffer,
                                &sender,
                                channels_size,
                                buf.samples(),
                                resampled_len,
                                &gain_function,
                            ) {
                                resampled_len += rl;
                            }
                        }
                    }
                    Err(Error::IoError(_)) | Err(Error::DecodeError(_)) => continue,
                    Err(_) => return Err(StreamErr::UnknownError),
                }
            }
        })
    }

    fn finished_flag(&self) -> Arc<AtomicBool> {
        self.finished.clone()
    }

    fn control_handle(&self) -> ControlHandle {
        self.control.clone()
    }

    fn get_input_info(&self) -> Result<Cow<'_, StreamerInputInfo>, StreamErr> {
        if let Some(input_info) = &self.streamer_input_info {
            Ok(Cow::Borrowed(input_info))
        } else {
            let (probed,_) = self.get_probe()?;
            let input_info = Self::get_input_info_from_probe(&probed)?;
            Ok(Cow::Owned(input_info))
        }
    }

    fn get_duration(&self) -> Option<u64> {
        // TODO this will be moved to the separate handler, see if we can do this, or have only static info
        let ii = self.get_input_info();
        if ii.is_err() {
            return None;
        }
        ii.unwrap().duration
    }

    fn get_output_info(&self) -> Option<DeviceOutputInfo> {
        self.output_info.clone()
    }
}
