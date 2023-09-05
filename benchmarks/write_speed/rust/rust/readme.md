when testing the rust writing options found out that:

1. the cargo::csv crate is very slow
2. the strategy of front loading to a string buffer like below is crazy slow as well
```rust
let mut batch = String::new();

for row_num in chunk_start..=chunk_end {
    batch.push_str(&format!("{}\n", row_num));
}

writer.write_all(batch.as_bytes())?;
writer.flush()?;
```
3. The strategy of using writeln! and running in chunks before a flush like below seems to be the fastest:
```rust
for row_num in chunk_start..=chunk_end {
    writeln!(writer, "{}", row_num)?;
}
writer.flush()?;
```
