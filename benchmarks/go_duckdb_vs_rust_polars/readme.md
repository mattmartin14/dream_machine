### Go and Duckdb Vs. Rust and Polars
#### Author: Matt Martin
#### Last Updated: 5/20/24

---

![mainphoto](./photos/gorust.jpg)

Duckdb and Polars have been on a collision course for a good bit now. Both offer an incredible way to work with and transform data in a very compact format e.g. the install for both is brain-dead easy and the overall footprint of each package is very small, considering what they can do. Most of the time when I reach for either of these packages, I do so through python. But there are times when I need to go with a compiled language, and up until a few weeks ago, the only way I knew to do this was in Rust. 

However... :smiley:, that has all changed now that I have found out that Go Lang can run Duckdb. This got me thinking...do I dare try to do a comparison that some might call sacrilegious of Go+Duckdb vs. Rust+Polars?

Before we go any further, let's address the elephant in the room and what this write-up is not intended to address. It is well-documented that as of today (5/20/24), Duckdb does struggle when the amount of data you want to use exceeds the ram on your machine. This write-up is not inteneded to test anything like that. This write-up will work with a dataset that is 5GB in size. The avaialable ram on my machine is 16GB, so it's well within the limits. Throughout my career, I have found that roughly 90% of the time, the data pipelines I'm building work with a 5GB or less dataset...shocking right? I thought it was all "big data"...sure the dataset in itself is large, but most of the time, i'm having to load or modify a slice/partition of the dataset. Even if a dataset overall is terabytes in size, a partition slice usually is way less.