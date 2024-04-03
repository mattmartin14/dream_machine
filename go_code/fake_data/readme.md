#### Fake Data Generator Using Cobra CLI
##### Author: Matt Martin
##### Data: 4/3/24

<hr></hr>
<h4>Overview</h4>

The cobra CLI is a great interface for organizing go lang CLI's. This program leverages the cobra cli recommended [best practices](https://pkg.go.dev/github.com/spf13/cobra#section-readme) of:

- CLI_Name > Verb > Noun > Adjective

to generate a fake dataset. Currently, this program is built to handle creating fake datasets in 2 file formats (JSON and CSV). You can pass in the number of rows you want to generate and the file name. Below are 2 examples that you can run on the CLI to generate data files:

```bash
fd create json --rows 10000 --filename somedata.json
fd create csv --rows 5000 --filename data.csv
```

<hr></hr>
<h4>Cobra CLI Remarks</h4>

I really enjoy using the cobra CLI as it standardizes the help menus accross all commands. It is easy to add flags and build robust CLI applications.

<hr></hr>
<h4>Other Thoughts</h4>

Down the road, I was thinking of expanding the CLI to do the following:

- Add "append" as an action in addition to the "create" verb
- Add "parquet" as an additional noun to support create parquet files

Enjoy!
