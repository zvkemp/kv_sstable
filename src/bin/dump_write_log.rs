use std::path::PathBuf;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let args: Vec<String> = std::env::args().collect();

    let path = args[1].clone();

    let pathbuf = PathBuf::from(path);

    println!("\n\n{:?}", pathbuf);
    dwkv::dump_write_log_to_println(&pathbuf).await;
}
