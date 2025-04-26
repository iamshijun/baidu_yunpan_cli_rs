use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};


pub async fn md5_sum_part(file_path: &str,start:u64,size:u64) -> Result<String, std::io::Error> { 
    let mut file = tokio::fs::File::open(file_path).await?;
    let mut hasher = md5::Context::new();
    let mut buffer = vec![0u8; 1024 * 1024];//1MB buffer

    file.seek(std::io::SeekFrom::Start(start)).await?;

    let mut remaining_size = size;
    while remaining_size > 0 {
        let bytes_read = file.read(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }
        hasher.consume(&buffer[..bytes_read]);
        remaining_size -= bytes_read as u64;
    }

    Ok(format!("{:x}", hasher.compute()))
}


pub async fn md5_sum(file_path: &str) -> Result<String, std::io::Error> { 
    let file = tokio::fs::File::open(file_path).await?;
    let size = file.metadata().await?.len();
    return md5_sum_part(file_path, 0, size).await;
}

pub struct SliceFileInfo<'a> {
    pub file_path: &'a str,//源文件路径
    pub size: u64,//actual size
    pub slice_size: u64,//chunk/slice size (!注意: 最后一个分片的大小size可能小于slice_size)
    pub seq : u64,
    //pub start: u64, //实际由 seq * slice_size 可以计算得到 就不冗余了
    pub md5: String,
}


impl Clone for SliceFileInfo<'_> {
    fn clone(&self) -> Self {
        SliceFileInfo {
            file_path: self.file_path,
            size: self.size,
            slice_size: self.slice_size,
            seq: self.seq,
            md5: self.md5.clone(),
        }
    }
    
}

impl SliceFileInfo<'_> {
   
    pub async fn read(&self) -> Result<Vec<u8>, std::io::Error> {
        let mut file = tokio::fs::File::open(self.file_path).await?;
        let start = (self.seq as u64) * self.slice_size;

        file.seek(tokio::io::SeekFrom::Start(start)).await?;

        let mut total_read = 0;
        let mut buffer = vec![0u8; self.size as usize];
        while total_read < self.size as usize {
            //一次读取 未必能全部读完 尤其是比较大的文件 (目前来看我的mac是每次2MB)
            let bytes_read = file.read(&mut buffer[total_read..]).await?;
            //println!("bytes_read:{} total_read:{}", bytes_read, total_read);

            if bytes_read == 0 {
                break; // 文件结束
            }
            total_read += bytes_read;
        }

        if total_read != self.size as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("read slice failed on seq:{}, total_read:{} expect:{}", self.seq, total_read, self.size),
            ))
        }
        Ok(buffer)
    }
}


pub async fn split_file2(file_path: &str,slice_size: u64,) -> Result<Vec<SliceFileInfo>, std::io::Error> {
    if !Path::new(file_path).exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("File not found: {}", file_path),
        ))
    }
    let metadata = tokio::fs::metadata(file_path).await?;
    if !metadata.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{} is not a file", file_path),
        ))
    }

    let total_file_size = metadata.len();
    let chunks = (total_file_size + slice_size - 1) / slice_size;

    let mut results = Vec::with_capacity(chunks as usize);

    let mut file = tokio::fs::File::open(file_path).await?;

    for i in 0..chunks { 

        let start = i * slice_size;
        let size = std::cmp::min(slice_size, total_file_size - start);

        let mut buffer = vec![0u8; size as usize];
        file.read_exact(&mut buffer).await?;

        let md5 = md5_sum_part(file_path, start, size).await?;
        results.push(SliceFileInfo { file_path, size, seq:i, slice_size, md5 })
    }
    Ok(results)
}

 /**
 * 将文件分割成指定大小的块
 *
 * @param file_path 要分割的文件路径
 * @param chunk_size 每个文件块的大小（以字节为单位）注意要大于4MB(严格来说第一个分片要大于等于4MB,小于4MB的直接一次就上传)
 * @param output_dir 输出目录
 * @return 包含每个块路径的向量
 */
pub async fn split_file(
    file_path: &str,
    chunk_size: u64,
    output_dir: &str,
) -> Result<Vec<PathBuf>, std::io::Error> {

    let path = Path::new(file_path);
    if !path.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("File not found: {}", file_path),
        ))
    }
    let metadata = tokio::fs::metadata(file_path).await?;
    if !metadata.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{} is not a file", file_path),
        ))
    }

    let total_file_size = metadata.len();
    let chunks = (total_file_size + chunk_size - 1) / chunk_size;

    let filename = path.file_name().unwrap().to_str().unwrap();

    let mut file = tokio::fs::File::open(file_path).await?;
    let mut chunk_paths = Vec::new();

    // 确保输出目录存在
    tokio::fs::create_dir_all(output_dir).await?;
    // 计算使用的缓冲区大小
    let buffer_size: usize = std::cmp::min(chunk_size, 100 * 1024) as usize;
    // 创建缓冲区(share)
    let mut buffer = vec![0u8; buffer_size];
 
    for i in 0..chunks {
       
        let mut left_size = std::cmp::min(chunk_size, total_file_size - (i * chunk_size));
        
        let slice_file_name = format!("{}_{}.part",filename, i);
        let chunk_path = PathBuf::from(output_dir).join(slice_file_name);

        let mut chunk_file = tokio::fs::File::create(&chunk_path).await?;
        chunk_paths.push(chunk_path); 

        while left_size > 0 {  
            //TODO  read source code 
            let to_read = std::cmp::min(left_size, buffer_size as u64) as usize;
            let bytes_read = file.read(&mut buffer[..to_read]).await?;
            if bytes_read == 0 {
                break; // 文件已读尽
            }
            // println!("bytes_read:{}, left_size:{}", bytes_read,left_size);
            chunk_file.write_all(&buffer[..bytes_read]).await?;
            
            //left_size = left_size.saturating_sub(bytes_read as u64);
            left_size -= bytes_read as u64;
        } 
    }
    Ok(chunk_paths)

}
