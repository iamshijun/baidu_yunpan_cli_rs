mod yunpan_service;
mod utils;

use yunpan_service::*;
use std::time::Instant;
use clap::Parser;


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    access_token: String,

    /// 要上传的文件路径
    #[arg(short, long)]
    file: String,

    /// 分片大小 (MB)
    #[arg(short, long, default_value_t = 10)]
    chunk_size: u64,

    /// 是否续传 TODO: 未实现
    #[arg(short, long, default_value_t = false)]
    resume: bool,
}


#[tokio::main]
async fn main() {
    let args = Args::parse();
    let file_path = &args.file;
    let chunk_size = args.chunk_size * 1024 * 1024;
    //let resume = args.resume;
    let access_token = args.access_token;

    let start_time = Instant::now();

    let yunpan_service = YunPanService::new(access_token);
 
    let result= yunpan_service.upload(
        CliUploadRequest::new(file_path, chunk_size)).await ;

    let elapsed = start_time.elapsed();
    println!("Upload took {:?}", elapsed);

    match result {
        Ok(_) => println!("Upload successful"),
        Err(e) => {
            match e {
                YunPanError::Reqwest(err) => println!("Reqwest error: {}", err),
                YunPanError::Io(err) => println!("Io error: {}", err),
                YunPanError::Biz(err) => println!("Biz error: {}", err),
                YunPanError::Serde(err) => println!("Serde error: {}", err),
            }
        }
    };  

}