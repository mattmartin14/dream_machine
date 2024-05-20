### Go and Duckdb Vs. Rust and Polars
#### Author: Matt Martin
#### Last Updated: 5/20/24

---

![mainphoto](./photos/gorust.jpg)

Duckdb and Polars have been on a collision course for a good bit now. Both offer an incredible way to work with and transform data in a very compact format e.g. the install for both is brain-dead easy and the overall footprint of each package is very small, considering what they can do. Most of the time when I reach for either of these packages, I do so through python. But there are times when I need to go with a compiled language, and up until a few weeks ago, the only way I knew to do this was in Rust. 

However... :smiley:, that has all changed now that I have found out that Go Lang can run Duckdb. This got me thinking...do I dare try to do a comparison that some might call sack-regligious of Go+Duckdb vs. Rust+Polars?? 