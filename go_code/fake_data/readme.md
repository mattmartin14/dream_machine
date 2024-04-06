#### Fake Data Generator Using Cobra CLI
##### Author: Matt Martin
##### Original Data: 4/3/24
##### Last Updated: 4/5/24

<hr></hr>
<h4>Overview</h4>

The cobra CLI is a great interface for organizing go lang CLI's. This program leverages the cobra cli recommended [best practices](https://pkg.go.dev/github.com/spf13/cobra#section-readme) of:

- CLI_Name > Verb > Adjective

to generate a fake dataset. Currently, this program is built to support the following flags:

| Flag Name | Default Value | Description |
| --------- | ------------- | ----------- |
| prefix    | data          | file name prefix e.g. "prefix[1,2,3...].csv |
| filetype  | none          | can be either json or csv |
| files     | none          | total number of files to create with the data |
| outputdir | none          | output directory to save files to; it must exist |
| rows      | none          | total number of rows to create |
| maxworkers | 5            | total number of go routines (parallel processes) in flight allowed |


```bash
fd create --filetype csv --maxworkers 6 --prefix fin_data_ --outputdir ~/test_dummy_data/fd --files 12 --rows 1000000
fd create --filetype json --maxworkers 4 --prefix merch_ --outputdir ~/test_dummy_data/fd --files 15 --rows 2000000
```

Keep in mind that if you set the number of workers too high, you could lock your machine; so be cautious not to overload it.

<hr></hr>
<h4>Cobra CLI Remarks</h4>

I really enjoy using the cobra CLI as it standardizes the help menus accross all commands. It is easy to add flags and build robust CLI applications.

<hr></hr>
<h4>Other Thoughts</h4>

Down the road, I was thinking of expanding the CLI to do the following:

- Add "append" as an action in addition to the "create" verb
- Add "parquet" as an additional filetype to support create parquet files

Enjoy!
