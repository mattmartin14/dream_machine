<h4>Using Docker for simple Python runtimes for DuckDb</h4>

#### Author: Matt Martin
#### Date: 2023-11-17

<h3>Overview</h3>
This repo demonstrates 2 simple ways to build a docker container to run python and duckdb. The first image (dk_slim) is the smallest footprint I've found that works to run python and duckdb. It's around 270MB. The second image (dk_notebook) is about 1GB in size and supports running jupyter notebooks. To run either container, simply copy the repo to your local workstation, and in terminal navigate to the folder of the image. As an example, you can naviage to the "dk_notebook" folder and then run this command:

```bash
docker-compose up
```

When you are done using the container, in terminal type CTRL+C to stop the container. It might take a few seconds, but once its stopped, you can then run the command below to fully remove the container:

```bash
docker-compose down
```

<hr></hr>
<h3>Interacting with the container in VS Code</h3>
Once you have started a container with the docker-compose up command, in VS code, you can attach to the container and start using it to run and test your code. To do so, make sure you have installed the VS Code Docker extension. Once installed, you will see the whale icon on the left hand side of VS code in the navigation bar. Click on it and you will see the containers that are running in the top navigation panel with a green arrow. When you right click on a running container, you will see an option of "Attach Visual Studio Code". Click on that and a new window will launch where VS code is inside the container. In the navigation panel, you should see a folder called "scripts" with the duckdb_test.ipynb notebook. You can click on that, and you should then be able to select a kernal for the notebook and start running your code. You might get prompted for a couple vs code extensions such as python intellisense, which is up to you if you want to install.