use datafusion::error::Result;
use datafusion::prelude::*;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;
use object_store::ObjectStore;

// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
// fetching results, using the DataFrame trait
#[tokio::main]
async fn main() -> Result<()> {

    // create local execution context
    let ctx = SessionContext::new();

    let BUCKET_NAME = "parquet-testing";

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(BUCKET_NAME)
        .with_region("eu-central-1")
        .with_access_key_id("AKIAIOSFODNN7EXAMPLE")
        .with_secret_access_key("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
        .with_endpoint("http://localhost:9000")
        .with_allow_http(true)
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
