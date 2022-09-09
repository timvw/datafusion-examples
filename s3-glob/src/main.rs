use datafusion::error::Result;
use datafusion::prelude::*;
use object_store::aws::AmazonS3Builder;
use std::env;
use std::sync::Arc;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::error::DataFusionError;
use glob::Pattern;
use object_store::ObjectMeta;
use object_store::path::Path;
use url::Url;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use itertools::Itertools;

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
        .register_object_store("s3", BUCKET_NAME, Arc::new(s3));

    // first need to figure out which files to read...

    let filename = format!("s3://{}/data/alltypes_p*n.parquet", BUCKET_NAME);
    println!("finding files that match: {}", filename);
    let s = filename.as_ref();

    let (prefix, glob) = match split_glob_expression(s) {
        Some((prefix, glob)) => {
            let glob = Pattern::new(glob)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            (prefix, Some(glob))
        }
        None => (s, None),
    };

    let full_glob = Pattern::new(&filename).unwrap();

    println!("prefix: {}, glob: {:?}", prefix, glob);

    let prefix_url = ListingTableUrl::parse(prefix)?;
    println!("prefix url: {}", prefix_url);

    let url = Url::parse(prefix).expect("should be an url");
    let prefix_path = Path::parse(url.path()).expect("should be URL safe");
    println!("prefix path: {}", prefix_path);

    let base_url = if prefix_path.as_ref().len() > 0 {
        url.as_str().strip_suffix(&format!("{}/", prefix_path.as_ref())).unwrap()
    } else {
        url.as_str()
    };
    println!("base: {}", base_url);

    let store = ctx.runtime_env().object_store(&prefix_url)?;
    println!("store: {}", store);

    let listing = store.list(Some(&prefix_path)).await.expect("should find files with prefix");

    let listing2: BoxStream<Result<ObjectMeta>> = listing.map_err(Into::into)
        .try_filter(move |meta| {
            let glob_match = match &glob {
                None => true,
                Some(gg) => {
                    if prefix_path.as_ref().len() > 0 {
                        let fg = format!("{}/{}", prefix_path.as_ref(), gg.as_str());
                        Pattern::new(&fg).unwrap().matches(meta.location.as_ref())
                    } else {
                        gg.matches(meta.location.as_ref())
                    }
                }
            };
            futures::future::ready(glob_match)
        })
        .boxed();

    let metas: Vec<ObjectMeta> = listing2.try_collect().await?;
    println!("items: {:?}", metas);

    let lp: Vec<_> = metas.iter().map(|om| {
        let fp = format!("{}{}", base_url, om.location.as_ref());
        ListingTableUrl::parse(fp).unwrap()
    }).collect();
    println!("files: {:?}", lp);

    let mut config = ListingTableConfig::new_with_multi_paths(lp);
    config = config.infer_options(&ctx.state.read()).await?;
    config = config.infer_schema(&ctx.state.read()).await?;



    let provider = ListingTable::try_new(config)?;

    let df = ctx.read_table(Arc::new(provider)).unwrap()
        .select_columns(&["id", "bool_col", "timestamp_col"])?
       .filter(col("id").gt(lit(1)))?;

    // print the results
    df.show().await?;



    Ok(())
}

const GLOB_START_CHARS: [char; 3] = ['?', '*', '['];

/// Splits `path` at the first path segment containing a glob expression, returning
/// `None` if no glob expression found.
///
/// Path delimiters are determined using [`std::path::is_separator`] which
/// permits `/` as a path delimiter even on Windows platforms.
///
fn split_glob_expression(path: &str) -> Option<(&str, &str)> {
    let mut last_separator = 0;

    for (byte_idx, char) in path.char_indices() {
        if GLOB_START_CHARS.contains(&char) {
            if last_separator == 0 {
                return Some((".", path));
            }
            return Some(path.split_at(last_separator));
        }

        if std::path::is_separator(char) {
            last_separator = byte_idx + char.len_utf8();
        }
    }
    None
}

/// Strips the prefix of this [`ListingTableUrl`] from the provided path, returning
/// an iterator of the remaining path segments
fn strip_prefix<'a, 'b: 'a>(
    prefix: &'a str,
    path: &'b Path,
) -> Option<impl Iterator<Item = &'b str> + 'a> {
    use object_store::path::DELIMITER;
    let path: &str = path.as_ref();
    let stripped = match prefix {
        "" => path,
        p => path.strip_prefix(p)?.strip_prefix(DELIMITER)?,
    };
    Some(stripped.split(DELIMITER))
}
