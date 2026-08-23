# Scratchpad

`test_server_owned_ducklake.sh` is a local-only experiment. It starts Quack with DuckLake attached in the server init session, then connects a client that only attaches Quack and tests a write/readback through `remote.query(...)` plus direct `remote.dl1.table` access.

Run it from the project root:

```bash
./scratchpad/test_server_owned_ducklake.sh
```

It uses a temporary directory and cleans it up automatically. It does not use AWS resources or modify the Terraform deployment.