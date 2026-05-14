1. run `go mod init quack_attack`
2. run `go get github.com/duckdb/duckdb-go/v2`
3. run `go mod tidy`
4. spin up quack remote server on another shell via:

```bash
duckdb
load quack;
create table data (id int);
call quack_serve('quack:localhost',token='yolo');
```
5. run `go run main.go`

6. validate results on remote shell:

```bash
from data;
```