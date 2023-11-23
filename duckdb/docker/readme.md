<h4>Using Docker for simple Python runtimes for DuckDb</h4>

#### Author: Matt Martin
#### Date: 2023-11-17

<h4>Overview</h4>
This repo demonstrates 2 simple ways to build a docker container to run python and duckdb. The first image (dk_slim) is the smallest footprint I've found that works to run python and duckdb. It's around 270MB. The second image (dk_notebook) is about 1GB in size and supports running jupyter notebooks. To run either container, simply copy the repo to your local workstation, and in terminal navigate to the folder of the image and run this command:

```bash
docker-compose up
```

When you are done using the container, in terminal type CTRL+C to stop the container. It might take a few seconds, but once its stopped, you can then run the command below to fully remove the container:

```bash
docker-compose down
```