### Go and Duckdb Vs. Rust and Polars
#### Author: Matt Martin
#### Last Updated: 5/20/24

---

<div style="text-align: center;">
  <img src="./photos/go_v_rust_v3.jpg" alt="header image">
</div>


#### Overview
Duckdb and Polars have been on a collision course for a good bit now. Both offer an incredible way to work with and transform data in a very compact format e.g. the install for both is brain-dead easy and the overall footprint of each package is very small, considering what they can do. Most of the time when I reach for either of these packages, I do so through python. But there are times when I need to go with a compiled language, and up until a few weeks ago, the only way I knew to do this was in Rust. 

However... :smiley:, that has all changed now that I have found out that Go can run Duckdb. This got me thinking...do I dare try to do a comparison that some might call sacrilegious of Go+Duckdb vs. Rust+Polars?

Before we go any further, let's address the elephant in the room and what this write-up is not intended to address. It is well-documented that as of 5/20/24, Duckdb does struggle when the amount of data you want to use exceeds the ram on your machine. This write-up is not inteneded to test anything like that. This write-up will work with a dataset that is 7GB in size. The avaialable ram on my machine is 16GB, so it's well within the limits. Throughout my career, I have found that roughly 90% of the time, the data pipelines I'm building work with a 5GB or less data...shocking right? I thought it was all "big data"...sure the dataset in itself is large, but most of the time, I'm having to load or modify a slice/partition of the dataset. Even if a dataset overall is terabytes in size, a partition slice usually is way less.

Now that we have that out of the way, let's continue.

---
#### The Setup

We will build a simple ETL pipeline in Go using DuckDB and the same pipeline in Rust with Polars. Both programs will follow the same path:

1. Ingest several CSV's
2. Aggregate up and group by a column
3. Add on a process timestamp
4. Export Aggregated Results to a single parquet file

--- 
#### Generating Test Dataset
Like I have shared before in previous write-ups, I'll use my [Go Lang Fake Data Generator](https://github.com/mattmartin14/dream_machine/blob/main/go_code/fake_data/readme.md) program to generate 7 GB of test data with the command below.

```Bash
fd create --filetype csv --maxworkers 8 --prefix test_data_ --outputdir ~/test_dummy_data/fd --files 20 --rows 50000000
```

Alright, we now have 50M records. Let's go crunch some data

---
#### Go+Duckdb Program

The Go code with DuckDb is as easy as it get's...I swear I don't have a bias here :smirk::

```GO
package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

func main() {

	start := time.Now()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	sql := `
		COPY 
		(
			SELECT FIRSTNAME AS FIRST_NAME
				, CURRENT_TIMESTAMP AS PROCESS_TS
				, COUNT(DISTINCT TXNKEY) AS KEY_CNT
				, SUM(NETWORTH) AS NET_WORTH_AMT
			FROM read_csv_auto('~/test_dummy_data/fd/test_data*.csv')
			GROUP BY 1
		) TO '~/test_dummy_data/fd/ducks.parquet'
		(FORMAT PARQUET)
	`

	_, err = db.Exec(sql)
	if err != nil {
		log.Fatal(err)
	}

	elapsed := time.Since(start)

	fmt.Println("Export complete")
	fmt.Printf("Elapsed time: %v ms\n", elapsed.Milliseconds())

}

```

With DuckDB, I did the entire pipeline in one shot. Some might not like that and say "how dare you! There is no way to test the individual parts of this...blah blah blah"...but hey, it works. 

---
#### Rust+Polars Program

As expected, the Rust version of this is definitely more involved. But I'd maybe argue in a good way, since Rust and Polars forces you to break the ETL into its true components of Extract, Transform, Load. Below is the rust code:

```RUST
use polars::prelude::*;
use std::error::Error;
use std::env;
use chrono::prelude::*;

fn main() -> Result<(), Box<dyn Error>> {

    let start_time = Utc::now();

    let home_dir = env::var("HOME")?;
    let csv_f_path = format!("{}/test_dummy_data/fd/*.csv", home_dir);
    
    let lf = LazyCsvReader::new(csv_f_path).finish()?;
  
    //transform
    let mut tsf = lf.clone().lazy()
        .group_by([col("FirstName")])
        .agg([
            col("TxnKey").n_unique().alias("TXK_KEY_CNT"),
            col("NetWorth").sum().alias("NET_WORTH_TOT"),
            col("TxnKey").count().alias("ROW_CNT"),
        ])
        .collect()
        .expect("Error getting dataframe created")
    ;

    // add current timestamp to the transformed dataframe
    let current_time_et = Local::now().with_timezone(&chrono_tz::America::New_York).naive_local();
    //println!("{}",current_time_et);
    let _result = tsf.with_column(
        Series::new("process_ts", vec![current_time_et])
    );

    //sample top 5 rows
    //println!("{}", tsf.head(Some(5)));

    // export result to parquet
    let par_f_path = format!("{}/test_dummy_data/fd/bears.parquet", home_dir);
    export_to_parquet(&mut tsf, &par_f_path)?;

    let end_time = Utc::now();
    let total_time = end_time - start_time;
    println!("Total time to process data: {:.2} seconds", total_time.num_seconds() as f64);

    Ok(())
}

//exports the dataframe to parquet
fn export_to_parquet(df: &mut DataFrame, par_f_path: &str) -> Result<(), PolarsError> {
    let mut file = std::fs::File::create(par_f_path)?;
    ParquetWriter::new(&mut file).finish(df)?;
    Ok(())
}
```

One thing I've found with Rust and Polars is that it appears the standard dataframe csv reader does not support a wildcard for the file names, and what you have to do is iterate over each file and stack the dataframes with a vector...which is a lot of work. The lazy reader though does support wildcards in the file names, which makes reading the files in much cleaner.

---
#### Results

Below are the run times for Go+Duckdb and Rust+Polars. Suprisingly, Go+Duckdb was significantly faster than Rust+Polars. I'm not sure if there is some other optimization trick I can do in Polars to make it go faster, but the results are what they are:

| Program | Total Time (Seconds) |
| ------- | -------------------  |
| Go + Duckdb | 4 seconds |
| Rust + Polars | 14 seconds |

---
#### Conclusion

Both approaches I think are fine for a data engineering pipeline. Even though Duckdb performed faster, I'm not really loosing sleep over it because we are still talking seconds. Of course, if this pipeline were to fire several thousand times a day, then I would consider taking Go over Rust. Additionally, Duckdb's SQL just makes the process so easy to author and get going. The rust code took a significantly longer time investment to get working correctly. Here's a link to each code set:

- [Go + DuckDb](./go_ducks/main.go)
- [Rust + Polars](./rust_bears/src/main.rs)


---
#### Other Thoughts

- In case you are curious how I generated this image, my prompt for Bing Image creator was "a gopher and a duck team vs. polar bear and crab team"
