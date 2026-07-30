# React Native library — scaffolding plan

Companion to `RN_TODO.md`. Ordered steps to scaffold a React Native Turbo Module
library around this crate using
[`uniffi-bindgen-react-native`](https://github.com/jhugman/uniffi-bindgen-react-native)
(`ubrn`, currently the 0.31.x line, built on UniFFI 0.31).

References in the form `(§N)` point to sections of `RN_TODO.md`.

---

## Phase 0 — Rust prep in this repo (do this first, §0)

Make the crate exportable **before** any RN tooling exists:

- [ ] `Cargo.toml`: add `[lib] crate-type = ["lib", "staticlib"]` (ubrn links a
      staticlib on both platforms)
- [ ] `Cargo.toml`: add `uniffi = "=X.Y.Z"` **exactly matching** the UniFFI
      version of the ubrn release being installed (§1 — version mismatch is the
      most common UniFFI build failure)
- [ ] Verify UniFFI's MSRV against `edition = "2024"` (§1)
- [ ] Create `src/ffi.rs` facade: handle-types-only exported surface over the
      internals — wrap `StreamPlayerImpl` / `Mixer` behind `&self` handles with
      `Mutex`-based interior mutability, following the existing
      `MixerHandle` / `ControlHandle` pattern (§2)
- [ ] Remove FFI-hostile types from the exported surface (§3):
  - `Vec<Box<dyn Streamer>>` → `Vec<Arc<dyn Streamer>>`, `Streamer: Send + Sync`,
    `#[uniffi::export(with_foreign)]`
  - `Box<dyn Fn>` callbacks → `#[uniffi::export(callback_interface)]` trait
  - keep `add_gain_function` Rust-side as an enum (`Linear | Log | Curve(...)`)
- [ ] API hygiene:
  - `seek(u64)` → `u32` ms or `f64` seconds (§8)
  - explicit `stop()` / `close()` on every object holding a cpal `Stream` (§6)
  - `uniffi::setup_scaffolding!()` in `lib.rs`
- [ ] Decide sync-vs-async construction for `SingleStreamer::new` (§5)

## Phase 1 — Scaffold the RN package (sibling directory)

```sh
cd ..
npx create-react-native-library@latest audio-learn-rn
# answers: Turbo module / C++ for Android & iOS / Vanilla example
cd audio-learn-rn
yarn && (cd example/ios && pod install)
yarn example start   # verify the template runs BEFORE adding Rust
```

- [ ] Package created and example app runs on both platforms unmodified

## Phase 2 — Wire ubrn to this crate

```sh
yarn add uniffi-bindgen-react-native @ubjs/core
```

- [ ] Add `ubrn:*` scripts to `package.json` (`ubrn:ios`, `ubrn:android`,
      `ubrn:clean`, …) per the ubrn getting-started guide
- [ ] `yarn ubrn:clean` — remove the template's `multiply()` native code
- [ ] Create `ubrn.config.yaml` at the package root pointing at the **local**
      crate (no `ubrn checkout` step needed):

```yaml
rust:
  directory: ../audio-learn
  manifestPath: Cargo.toml
android:
  targets: [arm64-v8a, x86_64]   # trim while iterating; restore 4 ABIs for release
  cargoExtras: ["--no-default-features", "--features", "mobile", "--profile", "release-mobile"]
ios:
  cargoExtras: ["--no-default-features", "--features", "mobile", "--profile", "release-mobile"]
```

- [ ] (If the crate later moves to its own repo: switch to `repo:` / `branch:`
      + `yarn ubrn:checkout` into `rust_modules/`, add `rust_modules/` and
      `*.a` to `.gitignore`)

## Phase 3 — Toolchain prereqs

- [ ] `rustup target add aarch64-linux-android armv7-linux-androideabi i686-linux-android x86_64-linux-android aarch64-apple-ios aarch64-apple-ios-sim`
- [ ] `cargo install cargo-ndk`
- [ ] Android **NDK r27+ pinned** (16 KB page-size Play Store requirement, §9)
- [ ] Xcode (for the xcframework build)

## Phase 4 — Build and smoke-test

```sh
yarn ubrn:ios        # staticlib → xcframework → generated TS/C++ → pod install
yarn ubrn:android    # per-ABI builds → CMake / jniLibs wiring
```

- [ ] Edit `example/src/App.tsx` to exercise **one cheap call** (construct a
      player handle, poll `get_play_time_ms()`)
- [ ] Wrap app registration in `uniffiInitAsync().then(...)` in
      `example/index.js`

## Phase 5 — Hand-written native layer (§4, §7)

Not scaffoldable — build alongside the generated module:

- [ ] **Notifier thread**: channel between cpal audio callbacks and JS
      delivery; never a synchronous hop from the audio thread
- [ ] **Android**: plumb `JavaVM` / `Context` into cpal (`JNI_OnLoad` or an
      `init_android()` call from Kotlin); foreground `Service` / Media3 for
      background playback
- [ ] **iOS**: Swift-side `AVAudioSession` category `.playback` before starting
      any stream; `UIBackgroundModes: audio` + `MPNowPlayingInfoCenter`
- [ ] Register both modules in one package (C++ engine module via
      `OnLoad.cpp` / `cxxModuleProvider()`; session module via Kotlin/Swift)

## Phase 6 — Iterate / release

- [ ] Loop: edit Rust → `yarn ubrn:android` / `yarn ubrn:ios` → Metro reload
- [ ] Restore all 4 Android ABIs for release builds
- [x] Trim `symphonia` features from `all` to needed formats before release (§9)
      — done via the `mobile` cargo feature + `release-mobile` profile in
      `Cargo.toml`; adjust the codec set in `mobile` to what the app ships

## Phase 7 — Expo testing app (§10)

Key constraint: the Rust is compiled **into the app binary** — Expo Go can
never load it. Requires a development build (`expo-dev-client`); every Rust
change means rebuilding that binary. New Architecture must stay on (default
since Expo SDK 52 / RN 0.76).

- [ ] Create the app:

```sh
npx create-expo-app@latest audio-learn-expo --template blank-typescript
cd audio-learn-expo
npx expo install expo-dev-client
```

- [ ] Install the library locally (autolinking picks up the podspec/gradle +
      generated C++ turbo module automatically):

```sh
# in the library repo first, so lib/ JS output exists:
cd ../audio-learn-rn && yarn prepare

# then in the Expo app (re-run after every library change):
npm install ../audio-learn-rn
```

- [ ] Add `app.plugin.js` in the app (native config autolinking can't express;
      ship it from the library itself later):

```js
const { withInfoPlist, withAndroidManifest } = require("expo/config-plugins");

module.exports = function withAudioLearn(config) {
  config = withInfoPlist(config, (c) => {
    c.modResults.UIBackgroundModes = ["audio"];
    return c;
  });
  config = withAndroidManifest(config, (c) => {
    const perms = (c.modResults.manifest["uses-permission"] ??= []);
    for (const name of [
      "android.permission.FOREGROUND_SERVICE",
      "android.permission.FOREGROUND_SERVICE_MEDIA_PLAYBACK",
    ]) {
      if (!perms.some((p) => p.$?.["android:name"] === name)) {
        perms.push({ $: { "android:name": name } });
      }
    }
    return c;
  });
  return config;
};
```

```json
// app.json
{ "expo": { "plugins": ["./app.plugin.js"] } }
```

- [ ] Build the dev client:

```sh
npx expo prebuild          # generates ios/ and android/; --clean after plugin changes
npx expo run:ios           # or: npx expo run:android
```

      (or `eas build --profile development` + `npx expo start --dev-client`)

- [ ] Initialize before registering the root component:

```ts
// index.ts
import { registerRootComponent } from "expo";
import { uniffiInitAsync } from "react-native-audio-learn";
import App from "./App";

uniffiInitAsync().then(() => registerRootComponent(App));
```

- [ ] Iteration rules:
  - **Rust change** → `yarn ubrn:ios/android` in the library → `npm install
    ../audio-learn-rn` again → **rebuild the dev client** (`expo run:ios`).
    The static lib is baked into node_modules; Metro reload is not enough.
  - **TS-only change** → Metro reload is enough.
  - **iOS silence (§7)**: until the Swift session module exists, cpal won't
    set `AVAudioSession` — smoke-test playback on Android first.
