use std::collections::HashMap;

use deltalake::*;
use deltalake::protocol::SaveMode;
use deltalake::schema::*;

// based on this repo: https://github.com/cmackenzie1/deltalake-examples-rs/tree/main/examples

// the try from uri stuff is hard to make sense of: https://docs.rs/deltalake/latest/deltalake/struct.DeltaOps.html#method.try_from_uri

#[tokio::main]
async fn main() -> Result<(), DeltaTableError> {
    // Use [`DeltaOps`] to create a new table at the specified URI.
    // The result with create the first commit of to the tables
    // `_delta_log/` location.
    // This will error if the table already exists.

    // The `memory://` URI is a special URI that will create a table in memory.
    // This is useful for testing and examples, but you can use any URI that
    // is supported by the underlying storage backends (e.g. `s3://`)
    let table = DeltaOps::try_from_uri("./data/http_requests")
        .await?
        .create()
        .with_table_name("http") // optional table name
        .with_comment("HTTP Request logs")
        .with_columns(vec![
            SchemaField::new(
                "timestamp".to_string(),
                SchemaDataType::primitive("timestamp".to_string()),
                true,
                HashMap::new(),
            )
        ])
        .with_save_mode(SaveMode::Append) // or `SaveMode::Overwrite`, `SaveMode::ErrorIfExists`, or `SaveMode::Ignore`
        .await?;

    println!("{}", table);

    Ok(())
}