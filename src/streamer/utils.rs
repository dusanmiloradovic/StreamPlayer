use std::collections::HashMap;
use std::sync::{Mutex};

pub fn execute_callback(
    callbacks: &Mutex<HashMap<u64, Box<dyn Fn() + Send>>>,
    callback_time: u64,
) {
    match callbacks.lock().unwrap().get(&callback_time) {
        Some(cb) => cb(),
        None => println!("Callback not found"),
    }
}