use aws_config::{BehaviorVersion, SdkConfig};
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;

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
    println!("upload id: {}", upload_id);

    Ok(())
}

async fn get_config() -> Result<SdkConfig, anyhow::Error> {
    let config = aws_config::load_defaults(BehaviorVersion::latest())
        .await;

    Ok(config)
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
        .await?;

    Ok(multipart_upload_res)
}
