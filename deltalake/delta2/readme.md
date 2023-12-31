### Using Docker with Iceberg

<h5>Author: Matt Martin</h5>
<h5>Date: 2023-11-06</h5>
<h5>Last Modified: 2023-11-11</h5>

<h4>Overview</h4>
Iceberg is a great platform for running ANSI SQL statements against open source files. Instead of having to load your data into a database, you can just store the files in an object store. This makes portability easy between cloud platforms. To get Iceberg to work though, you have to be very specific on the versions of spark and java that you have installed on your machine; otherwise, you will get very obscure and bizarre runtime errors. The easiest way to make this process repeatable is by leveraging a docker file, like the one in this repo.
<hr></hr>
<h5>Install Steps</h5>
The docker file pulls in the correct versions of spark and other dependencies to where we can in one container launch a python notebook that has all the requirements we need. To do this, follow these steps:
<h5></h5>

1. Fork this repo to your local workstation (you will need more than just the docker file as the docker file copies and uses a few other files in this repo to finalize)

2. Navigate to the folder of the dockerfile and docker-compose.yml file and then run the terminal command below:

```bash
docker-compose up
```

This command will build the image and launch a docker container.

3. When you start the container, you will see several outputs on the terminal screen. One is a URL to the jupyter server which is about halfway down as seen in the screenshot below. Grab that URL and paste it into a web broswer and launch it.

![bash](./photos/url.jpg)

4. You should now see the Juypter lab server in the web browser. There is a template notebook that contains sample Iceberg spark code to get you started called "iceberg_template.ipynb". Open that up and you should see the following:

![jup](./photos/jupyter2.jpg)


<h4>Matt's Side Notes</h4>

1. The biggest gotcha while trying to get Iceberg to work was dealing with errors on the iceberg runtime and extensions incompatability. You need to make sure that the versions of the iceberg spark runtime and iceberg spark sql extensions are identical. 

2. My test environment is on python 3.11 and pyspark 3.5. I have not tested this for backwards compatability on other versions.