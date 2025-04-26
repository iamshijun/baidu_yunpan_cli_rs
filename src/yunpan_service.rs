
use reqwest::Client;
use serde::{Deserialize, Serialize};
use url::Url;
use std::collections::hash_set;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::task::JoinSet;
// use tokio::io::AsyncWriteExt; 
use crate::utils::{split_file,split_file2, md5_sum ,SliceFileInfo};

// 定义自定义错误类型
#[derive(Debug)]
pub enum YunPanError {
    Io(std::io::Error),
    Reqwest(reqwest::Error),
    Serde(serde_json::Error),
    Biz(String),
}
impl From<std::io::Error> for YunPanError {
    fn from(err: std::io::Error) -> Self {
        YunPanError::Io(err)
    }
}
impl From<reqwest::Error> for YunPanError {
    fn from(err: reqwest::Error) -> Self {
        YunPanError::Reqwest(err)
    }
}

pub struct CliUploadRequest {
    file_path: String,
    chunk_size: u64,
    // continues: bool,
    // dir: Option<String>,
}
impl CliUploadRequest  {
    pub fn new(file_path: &str, chunk_size: u64) -> Self {
        if chunk_size < 4 * 1024 * 1024 {
            panic!("chunk_size must be greater than or equal to 4MB");
        }
        CliUploadRequest { 
            file_path: file_path.to_string(),
            chunk_size,
            // continues,
            // dir: None
        }
    }
}
 
pub struct YunPanService {
    access_token: String,
    client: Client,
}

struct UploadFile {
    file_path: String,
    file_name: String,
    // file_md5: String,
    file_size: u64,
}
impl UploadFile {
    pub async fn new(file_path: &str) -> Result<UploadFile, std::io::Error> {
        let metadata = tokio::fs::metadata(file_path).await?;

        let file_size = metadata.len();
        let file_name = Path::new(file_path).file_name().unwrap().to_str().unwrap().to_string();

        Ok(UploadFile {
            file_path: file_path.to_string(),
            // file,
            file_name,
            // file_md5,
            file_size,
        })
    } 

    /**
     * 逻辑分割文件 
     * @param slice_size 分割文件的大小 注意要大于4MB(严格来说第一个分片要大于等于4MB,小于4MB的直接一次就上传)
     * @return 分割文件的路径
     */
    pub async fn split2(&self,slice_size: u64) -> Result<Vec<SliceFileInfo>, std::io::Error> {
        let mut slice_size = slice_size;

        if self.file_size <= 4 * 1024 * 1024 {
            slice_size = self.file_size
        }

        let chunk_files = match split_file2(&self.file_path, slice_size).await {
            Ok(chunk_files) => chunk_files,
            Err(e) => {
                return Err(e);
            }
        };
  
        Ok(chunk_files)
    }

    /**
     * 物理分割文件
     * @param total_file_size 文件的大小
     * @param slice_size 分割文件的大小
     * @param dir 分割文件的保存目录
     * @return 分割文件的路径
    */
    pub async fn split(&self,chunk_size: u64) -> Result<Vec<SliceFile>, std::io::Error> {
        let chunk_paths = if self.file_size <= 4 * 1024 * 1024 {
            vec![PathBuf::from(self.file_path.clone())]
        }else {
            match split_file(&self.file_path, chunk_size, "/tmp").await {
                Ok(chunk_paths) => chunk_paths,
                Err(e) => {
                    return Err(e);
                }
            }
        };

        let mut set = JoinSet::new();

        for (index,chunk_path) in chunk_paths.into_iter().enumerate() {
            let file_path = chunk_path.to_str().unwrap().to_string();
            set.spawn(async move {
                let md5 = md5_sum(&file_path).await.unwrap();
                println!("index: {} md5: {}",index, md5);
                SliceFile { seq:index,file_path,md5,}
            });
        } 
        let mut tasks = Vec::new();
        while let Some(res) = set.join_next().await {
            let sf = match res {
                Ok(slice_file) =>  slice_file,
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,"")  //这里应该是其他错误类型，但是为了方便测试，这里直接返回NotFound
                    );
                }
            };
            tasks.push(sf);
        }
        Ok(tasks)

    }
}

struct SliceFile {
    seq: usize,
    file_path: String,//slice file path
    md5: String, //slice file md5 
}

#[derive(Debug, Serialize, Deserialize)]
struct XPanFilePreCreateRequest {
    path: String,
    size: u64,//上传文件时为文件大小, 上传目录为目录大小默认为0
    isdir: u8,//是否为目录 0 文件、1 目录 (似乎baidu接口支持bool)
    rtype: u32,//rename type
    autoinit: u32,
    uploadid: Option<String>, //断点续传时需要传入
    block_list: String, //json array
}

impl XPanFilePreCreateRequest {
    fn new(
        path: &str,
        size: u64,
        block_list: &Vec<String>,//每个block的 md5
    ) -> Self {
        //转换成json array 并覆盖掉block_list变量
        let block_list = serde_json::to_string(block_list).unwrap();

        XPanFilePreCreateRequest {
            path : path.to_string(),
            size,
            block_list ,
            isdir: 0,//暂时不支持目录上传
            autoinit: 1,
            rtype: 3,//统一覆盖
            uploadid: None,
        }
    }
}
#[derive(Debug, Serialize, Deserialize)]
struct XPanPrecreateResponse {
    errno: Option<u32>, //错误码 0：表示成功, -7:文件或目录名错误或无权访问,-10:容量不足..
    request_id: u64,
    #[serde(rename = "uploadid")]
    upload_id: String,
    return_type: u32,
    block_list: Vec<u32>,//注意返回的不是和请求的一样，需要上传的分片序号列表，索引从0开始
}

#[derive(Debug, Serialize, Deserialize)]
struct XPanFileCreateRequest {
    path: String,
    size: u64,
    isdir: u8,
    // 需要与预上传precreate接口中的rtype保持一致
    rtype: u32,//文件命名策略，默认 0 为不重命名，返回冲突 1 为只要path冲突即重命名 2 为path冲突且block_list不同才重命名 3 为覆盖
    block_list: String,
    uploadid: String,
    // local_ctime: Option<u64>,//客户端创建时间(精确到秒)，默认为当前时间戳
    // local_mtime: Option<u64>, //客户端修改时间(精确到秒)，默认为当前时间戳
    // is_revision: u8,//是否需要多版本支持 1为支持，0为不支持， 默认为0 (带此参数会忽略重命名策略)
    // mode: u8,//上传方式 默认为1; 1 手动、2 批量上传、3 文件自动备份  4 相册自动备份、5 视频自动备份
    //exif_info: Option<String>,//json字符串，orientation、width、height、recovery为必传字段，其他字段如果没有可以不传
}
impl XPanFileCreateRequest {
    pub fn new(
        path: &str,//上传到的路径
        size: u64,
        block_list: &Vec<String>,
        upload_id: &str,
    ) -> Self {
        //转换成json array 并覆盖掉block_list变量
        let block_list = serde_json::to_string(block_list).unwrap();
       
        XPanFileCreateRequest {
            path: path.to_string(),
            size,
            isdir: 0,
            rtype: 3,//统一覆盖
            block_list ,
            uploadid: upload_id.to_string()
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct XPanCreateResponse {
    errno: Option<u32>, //错误码 0：表示成功 
    fs_id: u64, //文件id
    md5: String, //文件的MD5，只有提交文件时才返回，提交目录时没有该值
    category: u32, //分类类型, 1 视频 2 音频 3 图片 4 文档 5 应用 6 其他 7 种子
    server_filename: Option<String>, //服务器文件名 -居然和文档不一样（不返回） 仅返回name而且和path一样
    path: String, //上传后使用的文件绝对路径
    size: u64, //文件大小
    ctime: u64, //创建时间
    mtime: u64, //修改时间
    isdir: u8, //是否为目录 0:为文件 1:为目录
}

#[derive(Debug, Serialize, Deserialize)]
struct XPanUploadResponse { 
    md5: String, //文件切片云端md5
    error_code: Option<u32>, //错误码  None为成功!(tmd api都不同格式的) 
    error_msg: Option<String>,
}

impl YunPanService {
    pub fn new(access_token: String) -> Self {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to build HTTP client");

        YunPanService {
            access_token,
            client,
        }
    }

    //预上传 doc : https://pan.baidu.com/union/doc/3ksg0s9r7
    async fn precreate(
        &self,
        request: &XPanFilePreCreateRequest,
    ) -> Result<XPanPrecreateResponse, YunPanError> {
        let url = format!(
            "https://pan.baidu.com/rest/2.0/xpan/file?method=precreate&access_token={}",
            self.access_token
        );

        let response: reqwest::Response = self.client.post(&url).form(request).send().await?;
    
        let raw_response_text = response.text().await?; // 直接转json response.json::<XPanPrecreateResponse>()

        //let text: String = response.text().await.expect("failed to get response text");
        match serde_json::from_str::<XPanPrecreateResponse>(&raw_response_text) {
            Ok(response) => {
                return if let Some(0) = response.errno {
                    Ok(response)
                }else {
                    Err(YunPanError::Biz(format!("precreate failed: {:?}", raw_response_text)))
                }
            },
            Err(e) => {
                eprintln!("serde_json::from_str failed on precreate response: {:?}",raw_response_text);
                return Err(YunPanError::Serde(e))//解析错误
            }
        }
    }


    //doc : https://pan.baidu.com/union/doc/nksg0s9vi
    async fn upload_slice(
        &self,
        path: &str,
        upload_id: &str,
        slice_file: &SliceFile,
    ) -> Result<XPanUploadResponse, YunPanError> {
     
        let mut url = Url::parse("https://c.pcs.baidu.com/rest/2.0/pcs/superfile2").unwrap();
        url.query_pairs_mut()
            .append_pair("method", "upload")
            .append_pair("access_token", &self.access_token)
            .append_pair("type", "tmpfile")
            .append_pair("path", path)
            .append_pair("uploadid", upload_id)
            .append_pair("partseq", &slice_file.seq.to_string());
 
        let file = tokio::fs::read(&slice_file.file_path).await?;

        let file_part  = reqwest::multipart::Part::bytes(file).file_name("filename");//必须指定file_name
        let form = reqwest::multipart::Form::new().part("file", file_part);

        let response = self.client.post(url).multipart(form).send().await?;

        //let response_body: XPanUploadResponse = response.json().await?;
        let raw_response_text = response.text().await?; 

        match serde_json::from_str::<XPanUploadResponse>(&raw_response_text) {
            Ok(response) => {
                return if let Some(0) = response.error_code.or_else(|| Some(0)) {//没有的话! 默认为0,有md5,request_id就行
                    Ok(response)
                } else{
                    Err(YunPanError::Biz(format!("upload slice failed on seq:{}, {:?}",slice_file.seq,raw_response_text)))
                }
            },
            Err(e) => {
                eprintln!("serde_json::from_str failed on upload_slice response: {:?}",raw_response_text);
                return Err(YunPanError::Serde(e))//解析错误
            }
        };
    }
    async fn upload_slice2(
        &self,
        path: &str,
        upload_id: &str,
        slice_file: &SliceFileInfo<'_>,
    ) -> Result<XPanUploadResponse, YunPanError> {

        let mut url = Url::parse("https://c.pcs.baidu.com/rest/2.0/pcs/superfile2").unwrap();
        url.query_pairs_mut()
            .append_pair("method", "upload")
            .append_pair("access_token", &self.access_token)
            .append_pair("type", "tmpfile")
            .append_pair("path", path)
            .append_pair("uploadid", upload_id)
            .append_pair("partseq", &slice_file.seq.to_string());
 
        let buffer = slice_file.read().await?;

        let file_part  = reqwest::multipart::Part::bytes(buffer).file_name("filename");//必须指定file_name
        let form = reqwest::multipart::Form::new().part("file", file_part);

        let response = self.client.post(url).multipart(form).send().await?;

        //let response_body: XPanUploadResponse = response.json().await?;
        let raw_response_text = response.text().await?; 

        match serde_json::from_str::<XPanUploadResponse>(&raw_response_text) {
            Ok(response) => {
                return if let Some(0) = response.error_code.or_else(|| Some(0)) {//没有的话! 默认为0,有md5,request_id就行
                    Ok(response)
                } else{
                    Err(YunPanError::Biz(format!("upload slice failed on seq:{}, {:?}",slice_file.seq,raw_response_text)))
                }
            },
            Err(e) => {
                eprintln!("serde_json::from_str failed on upload_slice response: {:?}",raw_response_text);
                return Err(YunPanError::Serde(e))//解析错误
            }
        };
    }
    
    

    //doc: https://pan.baidu.com/union/doc/rksg0sa17
    async fn create(
        &self,
        request: &XPanFileCreateRequest, 
    ) -> Result<XPanCreateResponse, YunPanError> {
        let url = format!(
            "https://pan.baidu.com/rest/2.0/xpan/file?method=create&access_token={}",
            self.access_token
        );
        println!("create::  upload_id:{} , request:{:?}", request.uploadid, request);
        let request_builder = self.client.post(&url).form(request);

        let response = request_builder.send().await?;
        let raw_response_text = response.text().await?; 

        match serde_json::from_str::<XPanCreateResponse>(&raw_response_text) {
            Ok(response) => {
                return if let Some(0) = response.errno {//errno=0表示上传成功,没有的情况或者不是0的情况都是有问题的
                    Ok(response)
                } else{
                    Err(YunPanError::Biz(format!("create failed: {:?}", raw_response_text)))
                }
            },
            Err(e) => {
                println!("serde_json::from_str failed on create response: {:?}",raw_response_text);
                return Err(YunPanError::Serde(e))//解析错误
            }
        };
    }

    pub async fn upload(&self, request: CliUploadRequest) -> Result<XPanCreateResponse, YunPanError> {
        let upload_file = UploadFile::new(&request.file_path).await?;
        let file_size = upload_file.file_size;
        //split(物理切割) vs split2(逻辑分割)
        let mut slice_files = upload_file.split2(request.chunk_size).await?;
        //排序 保证下面的block_list得到的顺序是按照seq来的,但是发送(upload_slice)的顺序随意 保证 block_list的位置即可
        slice_files.sort_by(|a, b| a.seq.cmp(&b.seq));
   
        let block_list: Vec<String> = slice_files.iter().map(|sf|  sf.md5.clone()).collect();

        let upload_file_path: String = Path::new("/apps/asitanokibou/")
                                            .join(upload_file.file_name.clone())
                                            .to_str().unwrap().to_string();
        //1. 预上传
        let pcreate_request  = 
            XPanFilePreCreateRequest::new(&upload_file_path, file_size, &block_list);

        let response =  self.precreate(&pcreate_request).await?;
        let upload_id = response.upload_id.as_str();
        println!("precreate::  upload_id:{}", upload_id );

        let slice_file_paths : hash_set::HashSet<String> = 
            slice_files.iter()
                .map(|sf| sf.file_path.to_string().clone())
                .collect();


        //2. 分片上传    //暂时先单线程上传 
        for slice_file in slice_files {
            println!("uploading slice:{} md5:{}", slice_file.seq,slice_file.md5.as_str());
            //upload_slice vs upload_slice2
            let upload_slice_response = self.upload_slice2(&upload_file_path, upload_id, &slice_file).await?;
            assert!(upload_slice_response.md5.eq(slice_file.md5.as_str()), "md5 not match");
            //注意 物理分割时 file_path为分片的路径, 逻辑分割时file_path为源文件路径!
        }

        // - 删除临时文件
        if slice_file_paths.len() > 1 {// =1的时候证明没有切片,即为源文件/ 或者是逻辑切分的所以这里的slice_file也是源文件的路径  
            println!("removing slice files: {:?}", slice_file_paths);
            for slice_path in slice_file_paths.iter() {
                tokio::fs::remove_file(&slice_path).await?;
            }
        }

        //3. 创建文件
        let create_request = XPanFileCreateRequest::new(
            &upload_file_path,
            file_size,
            &block_list,
            upload_id,
        );

        self.create(&create_request).await
    }
    
}
