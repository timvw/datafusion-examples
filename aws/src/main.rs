use datafusion::error::Result;
use datafusion::prelude::*;
use object_store::aws::AmazonS3Builder;
use std::env;
use std::sync::Arc;

// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
// fetching results, using the DataFrame trait
#[tokio::main]
async fn main() -> Result<()> {

    // create local execution context
    let ctx = SessionContext::new();

    let BUCKET_NAME = "datafusion-parquet-testing";

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(BUCKET_NAME)
        .with_region(env::var("AWS_REGION").unwrap())
        .with_access_key_id(env::var("AWS_ACCESS_KEY_ID").unwrap())
        .with_secret_access_key(env::var("AWS_SECRET_ACCESS_KEY").unwrap())
        .with_token(env::var("AWS_SESSION_TOKEN").unwrap())
        .build()?;

    ctx.runtime_env()
        .register_object_store("s3", "datafusion-parquet-testing", Arc::new(s3));

    let filename = format!("s3://{}/data/alltypes_plain.parquet", BUCKET_NAME);
    println!("need to fetch: {}", filename);

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
