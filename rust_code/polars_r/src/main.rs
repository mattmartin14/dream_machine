
use polars::prelude::*;
use polars_io::prelude::*;

fn main() {

    let mut df = df!(
        "foo" => &["Matt","Matt","Jane","Bill","Fred","Fred"],
        "bar" => &[1,3,4,5,6,7],
    ).expect("Failed to create Dataframe");

    //println!("Dataframe is:\n{}",df);

    let f_path = "/Users/matthewmartin/test_dummy_data/rust_par.parquet";

    let mut file = std::fs::File::create(f_path).unwrap();
    ParquetWriter::new(&mut file).finish(&mut df).unwrap();

    println!("here");

    // read the file back into another DF
    let mut file2 = std::fs::File::open(f_path).unwrap();
    let df2 = ParquetReader::new(&mut file2).finish().unwrap();

    println!("Dataframe 2 is:\n{}",df2);

    let df3 = df2
        .clone()
        .lazy()
        .select([
            sum("bar")
        ])
        .collect()?;
    println!("{}", df3);
    
    //still testing; throwing errors
    // do some aggregation
    // let df3 = df2.group_by("foo")
    //     .agg(vec![col("bar").count().alias("count")])
    //     .expect("failed to aggregate data")
    // ;

    // println!("{}", df3);

    println!("process complete");

}
