pub mod stream_player;

use std::fs::File;
use cpal::{default_host, FromSample, SizedSample};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use rubato::{Resampler, Fft, FixedSync, Indexing};
use audioadapter_buffers::direct::InterleavedSlice;
use std::sync::{Arc, Mutex};
use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::DecoderOptions;
use symphonia::core::errors::Error;
use symphonia::core::formats::{FormatOptions};
use symphonia::core::io::MediaSourceStream;
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;

fn main()  {
   // play_sample_on_device().expect("failed");
    let src = File::open("files/lost_in_the_city.mp3").expect("failed to open file");
    let mss = MediaSourceStream ::new(Box::new(src), Default::default());  // Create a probe hint using the file's extension. [Optional]
    let mut hint = Hint::new();
    hint.with_extension("mp3");

    // Use the default options for metadata and format readers.
    let meta_opts: MetadataOptions = Default::default();
    let fmt_opts: FormatOptions = Default::default();

    // Probe the media source.
    let mut probed = symphonia::default::get_probe()
        .format(&hint, mss, &fmt_opts, &meta_opts)
        .expect("unsupported format");

    if let Some(metadata_rev) = probed.metadata.get().as_ref().and_then(|m| m.current()) {
        for tag in metadata_rev.tags() {
            println!("[probe] {:?} = {}", tag.std_key, tag.value);
        }
    }

    let mut format = probed.format;
    let mut sample_buf = None;


    let track = format.default_track().expect("no audio track");

    // Use the default options for the decoder.
    // let dec_opts: AudioDecoderOptions = Default::default();
    //
    // // Create a decoder for the track.
    // let mut decoder = symphonia::default::get_codecs()
    //     .make_audio_decoder(
    //         track.codec_params.as_ref().expect("codec parameters missing").audio().unwrap(),
    //         &dec_opts,
    //     )
    //     .expect("unsupported codec");
    //
    // // Store the track identifier, it will be used to filter packets.
    let track_id = track.id;
    println!("Hey,{}", track_id);



    let dec_opts: DecoderOptions = Default::default();

    let mut decoder = symphonia::default::get_codecs()
        .make(&track.codec_params, &dec_opts)
        .expect("unsupported codec");

    loop {
        // Get the next packet from the media format.
        let packet = match format.next_packet() {
            Ok(packet) => packet,
            Err(Error::ResetRequired) => {
                // The track list has been changed. Re-examine it and create a new set of decoders,
                // then restart the decode loop. This is an advanced feature and it is not
                // unreasonable to consider this "the end." As of v0.5.0, the only usage of this is
                // for chained OGG physical streams.
                unimplemented!();
            }
            Err(Error::IoError(err))
            if err.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    // End of stream reached, exit the decode loop.
                    println!("End of stream reached.");
                    break;
                }
            Err(err) => {
                // A unrecoverable error occurred, halt decoding.
                panic!("{}", err);
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
                    println!("Decoded packet with spec: {:?}, capacity: {}", spec, capacity);
                }
                if let Some(buf) = &mut sample_buf {
                    buf.copy_interleaved_ref(_decoded);
                    let b =buf.samples();

                    // // The samples may now be access via the `samples()` function.
                    // sample_count += buf.samples().len();
                    // print!("\rDecoded {} samples", sample_count);
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
                // An unrecoverable error occurred, halt decoding.
                panic!("{}", err);
            }
        }
    }

}

fn play_sample_on_device() -> Result<(), anyhow::Error> {
    println!("Hello, world!");
    let host = default_host();
    let device = host.default_output_device().expect("no output device found");
    let config = device.default_output_config().expect("no default output config");
    match config.sample_format() {
        cpal::SampleFormat::F32 => play_sample::<f32>(&device, &config.into()),
        cpal::SampleFormat::I16 => play_sample::<i16>(&device, &config.into()),
        cpal::SampleFormat::U16 => play_sample::<u16>(&device, &config.into()),
        _ => panic!("Unsupported sample format"),
    }
}

pub fn play_sample<T>(device: &cpal::Device, config: &cpal::StreamConfig) -> Result<(), anyhow::Error>
where
    T: SizedSample + FromSample<f32>,
{
    let out_sample_rate = config.sample_rate;
    let out_channels = config.channels as usize;
    println!("output sample_rate: {}, channels: {}", out_sample_rate, out_channels);

    // --- Read WAV file ---
    let mut reader = hound::WavReader::open("files/example.wav")?;
    let spec = reader.spec();
    let in_sample_rate = spec.sample_rate;
    let in_channels = spec.channels as usize;

    let samples: Vec<f64> = match spec.sample_format {
        hound::SampleFormat::Float => {
            reader.samples::<f32>().map(|s| s.unwrap() as f64).collect()
        }
        hound::SampleFormat::Int => {
            let bits = spec.bits_per_sample;
            let max_val = (1u32 << (bits - 1)) as f64;
            reader.samples::<i32>().map(|s| s.unwrap() as f64 / max_val).collect()
        }
    };
    let nbr_input_frames = samples.len() / in_channels;
    println!(
        "file sample_rate: {}, channels: {}, frames: {}",
        in_sample_rate, in_channels, nbr_input_frames
    );

    // --- Resample with rubato ---
    let resampled_interleaved: Vec<f64> = if in_sample_rate != out_sample_rate {
        let chunk_size = 1024;
        let mut resampler = Fft::<f64>::new(
            in_sample_rate as usize,
            out_sample_rate as usize,
            chunk_size,
            2,               // sub_chunks
            in_channels,
            FixedSync::Both,
        )?;

        let input_adapter =
            InterleavedSlice::new(&samples, in_channels, nbr_input_frames)?;

        // Allocate output buffer (generous size)
        let ratio = out_sample_rate as f64 / in_sample_rate as f64;
        let estimated_output_frames = (nbr_input_frames as f64 * ratio * 1.1) as usize + 1024;
        let mut outdata = vec![0.0f64; estimated_output_frames * in_channels];
        let mut output_adapter =
            InterleavedSlice::new_mut(&mut outdata, in_channels, estimated_output_frames)?;

        let (_, nbr_output_frames) = resampler.process_all_into_buffer(
            &input_adapter,
            &mut output_adapter,
            nbr_input_frames,
            None,
        )?;

        outdata.truncate(nbr_output_frames * in_channels);
        println!("resampled: {} frames", nbr_output_frames);
        outdata
    } else {
        samples
    };

    // --- Adapt to output channel count and convert to f32 ---
    let resampled_frames = resampled_interleaved.len() / in_channels;
    let mut interleaved = Vec::with_capacity(resampled_frames * out_channels);
    for frame_idx in 0..resampled_frames {
        for ch in 0..out_channels {
            let src_ch = ch % in_channels;
            interleaved.push(resampled_interleaved[frame_idx * in_channels + src_ch] as f32);
        }
    }

    // --- Play ---
    let audio_data = Arc::new(Mutex::new((interleaved, 0usize)));
    let audio_data_clone = Arc::clone(&audio_data);

    let err_fn = |err| eprintln!("an error occurred on stream: {err}");
    let data_callback = move |out: &mut [T], _: &cpal::OutputCallbackInfo| {
        let mut guard = audio_data_clone.lock().unwrap();
        let (ref data, ref mut pos) = *guard;
        for sample in out.iter_mut() {
            if *pos < data.len() {
                *sample = T::from_sample(data[*pos]);
                *pos += 1;
            } else {
                *sample = T::from_sample(0.0f32);
            }
        }
    };

    let stream = device.build_output_stream(config, data_callback, err_fn, None)?;
    stream.play()?;
    std::thread::sleep(std::time::Duration::from_millis(10000));
    Ok(())
}