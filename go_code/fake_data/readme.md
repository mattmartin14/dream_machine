#### Fake Data Generator Using Cobra CLI
##### Author: Matt Martin
##### Original Data: 4/3/24
##### Last Updated: 4/5/24

<hr></hr>
<h4>Overview</h4>

The cobra CLI is a great interface for organizing go lang CLI's. This program leverages the cobra cli recommended [best practices](https://pkg.go.dev/github.com/spf13/cobra#section-readme) of:

- CLI_Name > Verb > Adjective

to generate a fake dataset. Currently, this program is built to suppor the following flags:

-- filetype (can be either csv or json)
-- files (this is the total count of files you want produced in parallel)
-- outputdir (the output directory where you want the files saved)
-- rows (total number of rows to generate)

```bash
fd create --filetype csv --files 15 --outputdir ~/test_data --rows 1000000
fd create --filetype json --files 15 --outputdir ~/test_data --rows 1000000
```

<hr></hr>
<h4>Cobra CLI Remarks</h4>

I really enjoy using the cobra CLI as it standardizes the help menus accross all commands. It is easy to add flags and build robust CLI applications.

<hr></hr>
<h4>Other Thoughts</h4>

Down the road, I was thinking of expanding the CLI to do the following:

- Add "append" as an action in addition to the "create" verb
- Add "parquet" as an additional filetype to support create parquet files

Enjoy!
