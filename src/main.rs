mod yunpan_service;
mod utils;

use yunpan_service::*;
use std::time::Instant;
use clap::Parser;


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = String::from(""))]
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
    let access_token = if !args.access_token.is_empty() {
        args.access_token
    } else {
        //1从环境变量中获取
        std::env::var("BAIDU_YUNPAN_ACCESS_TOKEN")
            .unwrap_or_else(|_e| {      
                //2从配置文件中获取 .baidu_yunpan (当前用户home目录下)
                dirs::home_dir()
                    .and_then(|h| std::fs::read_to_string(h.join(".baidu_yunpan")).ok() /*ok: Result->Option */)
                    //.unwrap_or_default()   //String::default() 空串 
                    .expect("Cannot find access_token,Please specify in command line or config file .baidu_yunpan") //没有指定的话, 直接panic
            })
    };
    let access_token = access_token.trim().to_string();
    //println!("access_token:{}", access_token);

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