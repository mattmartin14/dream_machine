focus on how to update the .zshrc file
- how to secure creds in a text file and then have them get cat'd into the .zshrc file
- updating the java version path to toggle between various jdk versions that have different support for spark 3 vs. spark 4


ecs fargate
- can reference the 2 articles i just put out


Spark and Lakehouses
- the 2 predominate lakehouses out there are Delta and Iceberg
- Delta is pretty much backed by databricks
- Iceberg is backed by everyone else
- both are pretty much feature complete at this point
- both support all the expected SQL DML ops such as INSERT/UPDATE/DELETE/MERGE
- neither reall have a competitive advantage over the other; it really boils down to vendor support

- for local spark with iceberg and delta, its pretty easy at this point
- for cloud development though, you are best served developing in a cloud IDE vs. local
-- to much engineering to do local dev for cloud spark managed platforms like aws glue
- wrote an article once showcasing just how involved this is: https://performancede.substack.com/p/spark-config-madness


Data Modeling
- how to handle journaled records; begin_ts, end_ts after the keys
- joins/where clause filter on that
- build out a duckdb scfipt th atmodels this
- discuss tradeoffs of 1-up surrogate keys vs. natural keys

Orchestration in Python
- a main script that calls sub script
- multi-processing script to run multiple jobs at once