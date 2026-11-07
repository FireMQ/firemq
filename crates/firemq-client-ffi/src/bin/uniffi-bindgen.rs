//! Generates the foreign-language bindings: `cargo run -p firemq-client-ffi --features cli
//! --bin uniffi-bindgen -- generate --library <cdylib> --language <lang> --out-dir <dir>`.

fn main() {
    uniffi::uniffi_bindgen_main()
}
