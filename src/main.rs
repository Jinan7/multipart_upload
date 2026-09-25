use std::{fs::File, io::Write, path::Path};

use anyhow::Context;
use aws_config::{BehaviorVersion, SdkConfig};
use aws_sdk_s3::{operation::{complete_multipart_upload::{CompleteMultipartUploadOutput}, create_multipart_upload::CreateMultipartUploadOutput, upload_part::UploadPartOutput}, primitives::{ByteStream, Length}, types::{CompletedMultipartUpload, CompletedPart}};
use rand::{RngExt, distr::Alphanumeric};
const CHUNK_SIZE: u64 = 1024 * 1024 * 5;
const MAX_CHUNKS: u64 = 10_000;
#[tokio::main]
async fn main() -> Result<(), anyhow::Error>{
    
    start_multipart_upload().await?;
    Ok(())
}

async fn start_multipart_upload() -> Result<(), anyhow::Error> {

    let bucket_name = "ub-s3-j1-iam";
    let key = "new_text_file.txt";
    let config = get_config().await?;
    let client = aws_sdk_s3::Client::new(&config);
    
    let multipartupload_res = create_multipart_upload(&client, bucket_name, key).await?;
    let upload_id = multipartupload_res.upload_id().ok_or(anyhow::anyhow!("No upload ID"))?;
    println!("{upload_id}");
    let _file = create_file(key)?;
    upload_parts(key, &client, bucket_name, key, upload_id).await?;


    Ok(())
}

async fn get_config() -> Result<SdkConfig, anyhow::Error> {
    let config = aws_config::load_defaults(BehaviorVersion::latest())
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
    let path = Path::new(file_path);
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
            .await
            .unwrap();

        let part_number = (chunk_index as i32) + 1;
        let upload_part_res = upload_part(client, bucket_name, key, upload_id, part_number, stream).await?;

        upload_parts.push(
            CompletedPart::builder()
                .e_tag(upload_part_res.e_tag.unwrap_or_default())
                .part_number(part_number)
                .build()
        );
    }

    let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
        .set_parts(Some(upload_parts))
        .build();

    complete_multipart_upload(client, bucket_name, key, upload_id, completed_multipart_upload).await?;

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

