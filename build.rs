// Copyright (C) 2022 Laurynas Biveinis
fn main() {
    cxx_build::bridge("src/ffi_cxx.rs")
        .flag_if_supported("-std=c++17")
        .compile("kirunadb-cxx");

    println!("cargo:rerun-if-changed=src/ffi_cxx.rs");
}
