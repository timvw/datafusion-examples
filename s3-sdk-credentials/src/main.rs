use aws_types::credentials::*;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::error::DataFusionError;
use datafusion::error::Result;
use datafusion::prelude::*;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use glob::Pattern;
use itertools::Itertools;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::ObjectMeta;
use std::env;
use std::sync::Arc;
use url::Url;

// This example demonstrates how to leverage [aws-sdk-rust](https://github.com/awslabs/aws-sdk-rust) to acquire credentials
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    let BUCKET_NAME = "datafusion-parquet-testing";

    let sdk_config = aws_config::load_from_env().await;
    let cp = sdk_config.credentials_provider().expect("cp");
    let creds = cp.provide_credentials().await.expect("creds");

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(BUCKET_NAME)
        .with_region(sdk_config.region().expect("no region").to_string())
        .with_access_key_id(creds.access_key_id())
        .with_secret_access_key(creds.secret_access_key())
        .with_token(creds.session_token().expect("no token").to_string())
        .build()?;

    ctx.runtime_env()
        .register_object_store("s3", BUCKET_NAME, Arc::new(s3));

    let filename = format!("s3://{}/data/alltypes_plain.parquet", BUCKET_NAME);

    // define the query using the DataFrame trait
    let df = ctx
        .read_parquet(filename, ParquetReadOptions::default())
        .await?
        .select_columns(&["id", "bool_col", "timestamp_col"])?
        .filter(col("id").gt(lit(1)))?;

    // print the results
    df.show().await?;

    Ok(())
}
