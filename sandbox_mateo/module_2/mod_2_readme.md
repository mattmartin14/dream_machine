focus on how to update the .zshrc file
- how to secure creds in a text file and then have them get cat'd into the .zshrc file
- updating the java version path to toggle between various jdk versions that have different support for spark 3 vs. spark 4


ecs fargate
- can reference the 2 articles i just put out


Spark Examples
- local iceberg
- local Delta
- pros/cons of each
- local dev for cloud deployments of spark is very involved; best served using a cloud managed spark environment for dev like aws glue notebooks or databricks

Data Modeling
- how to handle journaled records; begin_ts, end_ts after the keys
- joins/where clause filter on that
- build out a duckdb scfipt th atmodels this
- discuss tradeoffs of 1-up surrogate keys vs. natural keys

Orchestration in Python
- a main script that calls sub script
- multi-processing script to run multiple jobs at once