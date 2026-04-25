pub mod stream_player;
pub mod mp3_file_streamer;

use crate::mp3_file_streamer::stream_mp3_file;
use audioadapter_buffers::direct::InterleavedSlice;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{default_host, FromSample, SizedSample};
use rubato::{Fft, FixedSync, Resampler};
use std::sync::{Arc, Mutex};

fn main()  {
   // play_sample_on_device().expect("failed");
   stream_mp3_file("files/lost_in_the_city.mp3");
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