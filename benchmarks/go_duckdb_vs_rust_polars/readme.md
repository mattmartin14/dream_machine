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


---
#### Conclusion


---
#### Other Thoughts

- In case you are curious how I generated this image, my prompt for Bing Image creator was "a gopher and a duck team vs. polar bear and crab team"
