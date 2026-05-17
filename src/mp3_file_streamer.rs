use std::fs::File;
use cpal::default_host;
use cpal::traits::{DeviceTrait, HostTrait};
use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::DecoderOptions;
use symphonia::core::errors::Error;
use symphonia::core::formats::FormatOptions;
use symphonia::core::io::MediaSourceStream;
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;
use crate::stream_player::{new_stream_player, StreamPlayer};

pub fn stream_mp3_file(file_path: &str) {
    let src = File::open(file_path).expect("failed to open file");
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
    let track_id = track.id;

    let dec_opts: DecoderOptions = Default::default();

    let Some(sample_rate) = &track.codec_params.sample_rate else{
        panic!("no sample rate");
    };

    let channels= &track.codec_params.channels.unwrap();
    let track_channels_size =  channels.count();

    let host = default_host();
    let device = host.default_output_device().expect("no output device found");
    let supported = device
        .supported_output_configs()
        .expect("error querying output configs")
        .any(|config| config.channels() == track_channels_size as u16);
    if !supported {
        panic!("device does not support this channel count");
    }
    let mut decoder = symphonia::default::get_codecs()
        .make(&track.codec_params, &dec_opts)
        .expect("unsupported codec");
    let mut stream_player = new_stream_player(*sample_rate, track_channels_size as u16);

    stream_player.start();
    loop {
        let packet = match format.next_packet() {
            Ok(packet) => packet,
            Err(Error::ResetRequired) => {
                unimplemented!();
            }
            Err(Error::IoError(err))
            if err.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    // End of stream reached, exit the decode loop.
                    println!("End of stream reached.");
                    stream_player.stop();
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
                    stream_player.push_samples(b);
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