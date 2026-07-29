# React Native port — TODO / gotchas

Target: expose this crate as a React Native library via
[`uniffi-bindgen-react-native`](https://github.com/jhugman/uniffi-bindgen-react-native) (`ubrn`),
which generates a C++ JSI Turbo Module from UniFFI interfaces.

Ordered roughly by how much work each item costs.

---

## 0. Suggested first step

Before wiring up any RN build, add a `ffi` module that defines the **exported**
surface — handle types, callback traits, plain enums/records — as a facade over
the existing internals. That forces the `&mut self` / `Box<dyn Fn>` redesign
below while it is still just a Rust refactor, rather than after the RN build is
in the loop.

---

## 1. Maturity / versioning

- The docs still carry an explicit *"early development, should not yet be used
  in production"* warning.
- A rename to `uniffi-bindgen-javascript` is planned.
- Pin `ubrn` and the `uniffi` crate to **exactly matching versions**. Mismatched
  uniffi versions between the crate and the bindgen is the most common build
  failure in UniFFI-land.
- Check `uniffi`'s MSRV against `edition = "2024"` in `Cargo.toml`.

## 2. `&mut self` does not cross the FFI

Exported object methods take `&self` or `Arc<Self>`. Currently broken:

- `StreamPlayerImpl::start(&mut self)` — `src/stream_player.rs:91`
- `Mixer::set_normalize_gain(&mut self)` — `src/streamer/mixer.rs:160`
- `Mixer::add(&mut self, ...)` — `src/streamer/mixer.rs:174`
- `Mixer::set_weight(&mut self, ...)` — `src/streamer/mixer.rs:272`

The codebase already has the answer: `MixerHandle` and `ControlHandle` are
`&self`-only with interior mutability. **Those are the UniFFI surface**, not
`Mixer` / `StreamPlayerImpl`. Plan on the exported layer being handle types
exclusively.

## 3. `Box<dyn Trait>` and closures do not exist in UniFFI

- `PlayListStreamer::new(streamers: Vec<Box<dyn Streamer>>)` —
  `src/streamer/playlist.rs:34` → must become `Vec<Arc<dyn Streamer>>`, and
  `Streamer` needs `Send + Sync` (currently `Send` only,
  `src/streamer/mod.rs:156`), plus `#[uniffi::export(with_foreign)]` if JS
  should ever implement it.
- `add_callback(after: Duration, callback: Box<dyn Fn() + Send>)` —
  `src/streamer/mod.rs:54,77` → there is no closure type. Convert to a
  callback-interface trait:

  ```rust
  #[uniffi::export(callback_interface)]
  pub trait StreamerCallback: Send + Sync {
      fn on_fire(&self);
  }
  ```

- `add_gain_function(Arc<dyn Fn(usize) -> f32 + Send + Sync>)` —
  `src/streamer/mod.rs:146` → **do not expose.** It is invoked per-sample on the
  audio thread; each call would be a JSI hop. Keep it Rust-side as an enum
  (`Linear | Log | Curve(Vec<(u32, f32)>)`) and let JS pick the variant.

## 4. Threading — the real hazard

`ubrn` does same-thread JS→Rust calls, but Rust→JS callbacks must hop to the JS
thread via the `CallInvoker`.

- **Never let a cpal audio callback reach JS synchronously.** The
  `execute_callback` / `schedule_callback` path (`src/streamer/utils.rs:4`,
  `src/streamer/mixer.rs:307`) must push into a channel drained by a dedicated
  notifier thread. A blocking hop from the audio thread means xruns.
- Callback *delivery* timing is non-deterministic. Keep sample-accurate
  scheduling entirely in Rust; JS gets "this happened", never "do this now".
- Position updates: do not push at frame rate. Throttle to ~5 Hz, or let JS poll
  `get_play_time_ms()` (`src/stream_player.rs:217`) from `requestAnimationFrame`
  — polling is cheap since calls are same-thread.

## 5. Sync calls block the JS thread

`SingleStreamer::new` (`src/streamer/single.rs:121`) does symphonia probe +
format detection = I/O + parsing. On the JS thread that is a visible hitch.

- Option A: mark it `async` (UniFFI async fns become JS Promises) — but this
  pulls in an async runtime (`#[uniffi::export(async_runtime = "tokio")]`),
  which the current crossbeam-based crate avoids.
- Option B: keep it sync but do construction off-thread behind a callback.

## 6. GC-driven `Drop` is not deterministic

Objects are `Arc`-backed with Hermes finalizers. A `StreamPlayer` holding a cpal
`Stream` can be collected long after JS drops the reference — audio keeps
playing, device stays open. Always expose an explicit `stop()` / `close()` and
require JS to call it.

## 7. cpal on mobile — UniFFI will not help here

This is where a hand-written Kotlin/Swift Turbo Module is still needed alongside
the generated one.

- **Android**: cpal's Oboe/AAudio backend needs a `JavaVM` + `Context`. There is
  no `android_main` in an RN app, so they must be plumbed in explicitly —
  either `JNI_OnLoad` or an `init_android(context)` call from Kotlin at startup.
- **iOS**: cpal will not configure `AVAudioSession`. Without setting category
  `.playback` from Swift *before* starting the stream there is silence, and no
  background audio regardless of Info.plist.
- **Background playback**: Android foreground `Service` + Media3
  `MediaSessionService`; iOS `UIBackgroundModes: audio` +
  `MPNowPlayingInfoCenter` for lock-screen controls. None of this is reachable
  from Rust.
- **Audio-thread priority**: threads cpal spawns do not automatically get
  real-time priority on Android. Expect tuning.

### Module split

Turbo Native Modules are chosen **per module at registration time**, not per
library, so both can ship in one package:

| Module | Language | Registered in |
|---|---|---|
| engine (this crate) | Rust → generated C++ | `cxxModuleProvider()` in `OnLoad.cpp` / `RCTModuleProvider` |
| playback session / lifecycle | Kotlin + Swift | `BaseReactPackage.getModule()` / ObjC module |

Note a pure C++ Turbo Module has **no `Context` and no `Activity`** — another
reason the session module must be Kotlin/Swift.

## 8. Numeric types in TS

`u64` / `i64` map to `bigint`, which is awkward in RN app code.

- `ControlHandle::seek(time: u64)` — `src/streamer/mod.rs:132` → prefer `u32` ms
  or `f64` seconds.
- `Duration` is a UniFFI builtin, so `add_callback(after: Duration)` is fine.

## 9. Build / packaging

- `cargo-ndk` for 4 Android ABIs; xcframework for iOS device + simulator.
- Pin the NDK version.
- **16 KB page-size requirement** for Play Store (NDK r27+).
- `symphonia = { features = ["all"] }` compiles every codec — several MB per
  ABI in the shipped APK/IPA. Trim to the formats actually needed.

## 10. Expo

- Requires a dev build with a config plugin; will not run in Expo Go.
- New Architecture is mandatory (default since RN 0.76).
