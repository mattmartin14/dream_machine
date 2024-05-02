### Using Go Lang to do some simple ETL
#### Author: Matt Martin
#### Date; 5/1/24

<hr>
<h3>Overview</h3>
Daniel Beach decided to pour some salt in the wound of a post he put on LinkedIn late in the last week of April 2024, saying that Rust has won the data engineering battle against Go Lang. I totally agree with him in that regard since rust is faster, has a safer architecture, and has support for data frame packages like Polars. Go Lang does not have a native data frame package. This got me thinking though...could I have Go read in a CSV file, do some basic transformations, and then output the results of the transormed data to a parquet file? Challenge accepted!

<hr>
<h3>How I did it</h3>
First, I needed to prep the data.... using FD link here

the code
main module
csv writer
parquet writer

conclusion
yes you can use go for ETL...its a lot more involved than reading data to a polars data frame but hey..it works

other thoughts
can it be more dynamic...maybe

