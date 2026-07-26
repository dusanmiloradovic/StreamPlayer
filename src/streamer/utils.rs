pub fn f_fadeout_log(x: usize, cutoff_samples: usize) -> f32 {
    if x >= cutoff_samples {
        return 0.0;
    }
    let t = x as f32 / cutoff_samples as f32;
    let floor_db = -60.0_f32;
    let db = floor_db * t;
    10.0_f32.powf(db / 20.0)
}

pub fn f_fadein_log(x: usize, cutoff_samples:usize) -> f32 {
    if x >= cutoff_samples {
        return 1.0;
    }
    let t = x as f32 / cutoff_samples as f32;
    let floor_db = -60.0_f32;
    let db = floor_db * (1.0 - t);
    10.0_f32.powf(db / 20.0)
}

pub fn f_fadeout_linear(x: usize, cutoff_samples:usize) -> f32 {
    if x < cutoff_samples {
        return (cutoff_samples - x) as f32 / cutoff_samples as f32;
    }
    0.0
}

pub fn f_fadein_linear(x: usize, cutoff_samples:usize) -> f32 {
    if x < cutoff_samples {
        return x as f32 / cutoff_samples as f32;
    }
    1.0
}

