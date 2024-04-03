#### Fake Data Generator Using Cobra CLI
##### Author: Matt Martin
##### Data: 4/3/24

<hr></hr>
<h4>Overview</h4>

The cobra CLI is a great interface for organizing go lang CLI's. This program leverages the cobra cli recommended [best practices](https://pkg.go.dev/github.com/spf13/cobra#section-readme) of:

- CLI_Name > Verb > Noun > Adjective

to generate a fake dataset. Examples you can run in terminal are:

```bash
fd create json --rows 10000 --filename somedata.json
fd create csv --rows 5000 --filename data.csv
```
