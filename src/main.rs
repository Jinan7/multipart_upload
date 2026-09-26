use std::{fs::File, io::Write, path::Path, sync::{Arc, Mutex}, thread::JoinHandle};
use anyhow::Context;
use aws_config::{BehaviorVersion, SdkConfig, retry::RetryConfig};
use aws_sdk_s3::{operation::{complete_multipart_upload::CompleteMultipartUploadOutput, create_multipart_upload::CreateMultipartUploadOutput, upload_part::UploadPartOutput}, primitives::{ByteStream, Length, event_stream::HeaderValue::Uuid}, types::{CompletedMultipartUpload, CompletedPart}};
use rand::{RngExt, distr::Alphanumeric};
use watchexec::{Watchexec, error::CriticalError};
use watchexec_events::{Event, Tag::FileEventKind, filekind::FileEventKind::{Modify, Remove}};
use watchexec_signals::Signal;
const CHUNK_SIZE: u64 = 1024 * 1024 * 5;
const MAX_CHUNKS: u64 = 10_000;
#[tokio::main]
async fn main() -> Result<(), anyhow::Error>{
    
    start_multipart_upload().await?;
    Ok(())
}

async fn start_multipart_upload() -> Result<(), anyhow::Error> {

    let bucket_name = "ub-s3-j1-iam";
    let key = format!("new_text_file-{}.txt", uuid::Uuid::new_v4());
    
    let config = get_config().await?;
    
    let client = aws_sdk_s3::Client::new(&config);
    
    let multipartupload_res = create_multipart_upload(&client, bucket_name, &key).await?;
    let upload_id = multipartupload_res.upload_id().ok_or(anyhow::anyhow!("No upload ID"))?;
    println!("{upload_id}");
    let _file = create_file(&key)?;
    upload_parts(&key, &client, bucket_name, &key, upload_id).await?;


    Ok(())
}

async fn get_config() -> Result<SdkConfig, anyhow::Error> {
    let retry_config = RetryConfig::standard()
        .with_max_attempts(10);
    let config = aws_config::defaults(BehaviorVersion::latest())
        .retry_config(retry_config)
        .load()
        .await;

    Ok(config)
}

async fn upload_parts(
    file_path: &str,
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    key: &str,
    upload_id: &str,
) -> Result<(), anyhow::Error> {
    //get path and set file watcher on path
    let path = Path::new(file_path);
    //atomic boolean to track if file has been modified
    let modified = Arc::new(Mutex::new(false));
    let wx = get_file_watcher(path, Arc::clone(&modified)).await?;

    let file_size = tokio::fs::metadata(path)
        .await
        .expect("file not found")
        .len();

    let mut chunk_count = (file_size / CHUNK_SIZE) + 1;
    let mut size_of_last_chunk = file_size % CHUNK_SIZE;

    if size_of_last_chunk == 0 {
        size_of_last_chunk = CHUNK_SIZE;
        chunk_count -=1;
    }

    if file_size == 0 {
        return Err(anyhow::anyhow!("invalid file"));
    }

    if chunk_count > MAX_CHUNKS {
        return Err(anyhow::anyhow!("File too large"))
    }

    let mut upload_parts: Vec<aws_sdk_s3::types::CompletedPart> = Vec::new();

    //start file watcher
    let handle = wx.main();
    for chunk_index in 0..chunk_count {
        let chunk_size = if chunk_count - 1 == chunk_index {
            size_of_last_chunk
        } else {
            CHUNK_SIZE
        };

        let stream = ByteStream::read_from()
            .path(path)
            .offset(chunk_index * CHUNK_SIZE)
            .length(Length::Exact(chunk_size))
            .build()
            .await?;

        let part_number = (chunk_index as i32) + 1;
        let upload_part_res = upload_part(client, bucket_name, key, upload_id, part_number, stream).await?;

        upload_parts.push(
            CompletedPart::builder()
                .e_tag(upload_part_res.e_tag.unwrap_or_default())
                .part_number(part_number)
                .build()
        );

        //abort upload if file was modified during upload
        if *modified.lock().unwrap() == true {
            //abort upload
            abort_multipart_upload(client, bucket_name, upload_id, key).await?;          
            close_file_watcher(wx, handle).await?;           
            //early return an error
            return Err(anyhow::anyhow!("Error reading file"));
        }
    }

    let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
        .set_parts(Some(upload_parts))
        .build();

    complete_multipart_upload(client, bucket_name, key, upload_id, completed_multipart_upload).await?;
    close_file_watcher(wx, handle).await?;
    
    Ok(())
}

async fn close_file_watcher(
    watcher: Arc<Watchexec>,
    handle: tokio::task::JoinHandle<Result<(), CriticalError>>
) -> Result<(), anyhow::Error>{

    //send terminate event to file watcher
    watcher.send_event(Event::default(), watchexec_events::Priority::Urgent).await?;
    //block on file watcher to finish thread
    handle.await?;
    Ok(())
}
async fn upload_part(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: ByteStream
) -> Result<UploadPartOutput, anyhow::Error> {
    println!("Uploading Part: {}", part_number);
    let result = client
        .upload_part()
        .key(key)
        .bucket(bucket_name)
        .upload_id(upload_id)
        .body(body)
        .part_number(part_number)
        .send()
        .await
        .context(format!("Error uploading part {}", part_number))?;
    println!("Completed Part Upload: {}", part_number);
    Ok(result)
}

async fn create_multipart_upload(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> Result<CreateMultipartUploadOutput, anyhow::Error> {

    let multipart_upload_res: CreateMultipartUploadOutput = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .context("Error creating multipart upload")
        ?;

    Ok(multipart_upload_res)
}

async fn abort_multipart_upload(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    upload_id: &str,
    key: &str
) -> Result<(), anyhow::Error> {

    let res = client
        .abort_multipart_upload()
        .bucket(bucket)
        .upload_id(upload_id)
        .key(key)
        .send()
        .await
        .context("Error aborting multipart upload")?;

    Ok(())
}

async fn complete_multipart_upload(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    completed_multipart_upload: CompletedMultipartUpload
) -> Result<CompleteMultipartUploadOutput, anyhow::Error> {

    Ok(client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await?)
}

fn create_file(
    key: &str
) -> Result<File, anyhow::Error> {

    let mut file = File::create(key).expect("could not create file");

    while file.metadata().unwrap().len() <= CHUNK_SIZE * 4 {
        let rand_string: String = rand::rng()
        .sample_iter(Alphanumeric)
        .take(256)
        .map(char::from)
        .collect();
        
        let return_string = "\n".to_string();

        file.write_all(rand_string.as_ref()).expect("Error writing to file");
        file.write_all(return_string.as_ref()).expect("Error writing to file");
    }

    Ok(file)
}

async fn get_file_watcher(
    file_path: &Path,
    modified: Arc<Mutex<bool>>
) -> Result<Arc<Watchexec>, anyhow::Error> {

    let wx = Watchexec::new(move |mut action| {

        for event in action.events.iter() {

            for tag in event.tags.iter() {

                if let FileEventKind(event_kind) = tag {

                    match event_kind {
                        Modify(_) => *modified.lock().unwrap() = true,
                        Remove(_) => *modified.lock().unwrap() = true,
                        _ => ()
                    }
                }
            }
        }

        if action.signals().any(|sig|{ matches!(sig, Signal::Interrupt | Signal::Terminate)}) {
            action.quit()
        }

        action

    })?;

    wx.config.pathset([file_path]);

    Ok(wx)
}


#[cfg(test)]
mod test {

    use std::path::Path;

use aws_sdk_s3::primitives::{ByteStream, Length};

use crate::{CHUNK_SIZE, create_file};


    #[tokio::test]
    async fn generate_file_of_5_chunks() {

        let key = "testfile.txt";
        let file = create_file(key).expect("error creating file");
        let path = Path::new(key);
        let file_size = tokio::fs::metadata(path)
            .await
            .unwrap()
            .len();
        dbg!(file_size);
        assert!(file.metadata().unwrap().len() > CHUNK_SIZE * 4);
    }

    #[tokio::test]
    async fn what_happens_when_file_is_overwriten_during_upload() {

        let key = "testfile.txt";
        let file = create_file(key).expect("error creating file");
        let _ = dbg!(file.metadata().unwrap().created());
        let path = Path::new(key);
        ByteStream::read_from()
            .path(path)
            .length(Length::Exact(CHUNK_SIZE))
            .build()
            .await
            .unwrap();

        // let _  = create_file(key).expect("error creating file");
        let _ = dbg!(file.metadata().unwrap().modified());
        ByteStream::read_from()
            .path(path)
            .offset(CHUNK_SIZE)
            .length(Length::Exact(CHUNK_SIZE))
            .build()
            .await
            .unwrap();


    }
}

