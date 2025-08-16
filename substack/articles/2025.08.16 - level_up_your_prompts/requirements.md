## Project Name
Comparing Speed of Duckdb UDF's Strategies

### Overview
The goal of this project is to test a theory I have about duckdb user defined functions and if we can make them more performant.

I've noticed that when a user defined function written in python is registered with duckdb, if the UDF is complex and the dataset is large, then the performance is rather slow. 

What I want to test though is - could we improve the performance if we instead had the function written in rust, and then exposed as a python function that we then use with duckdb?

### Requirements
Using Duckdb, build a script that creates a large table with 100M rows of data. The data needs to have some complex strings in it that we will want to parse. The UDF will do some parsing of the string to calculate something - i'll leave up to you what that calculation should be, but make it something that would be realistic in the real world.

We will want both a pure python version of the UDF created as well as rust based version that is exposed in python.

We will then want to run a benchmark of 5 runs using the pure python version of the UDF vs. the rust based version and compare the processing times of each on a bar chart showing the runs. For that, let's use matplot lib to plot the bar chart of the runs for each version of the UDF.


### Permissions You Have
You can create the necessary folders you need for the rust project and the python scripts; make sure the folder structure is easy to follow and makes sense here.


### Deliverables
1. a duckdb database with a duckdb table with 100M rows of data
2. a python UDF registered to duckdb
3. a rust base UDF exposed as a python function registered to duckdb
4. the benchmark query that runs 5 times for the python based udf and the rust based udf
5. a bar chart illustrating the run time results. You decide if we should illustrate it in seconds or milliseconds on the y axis depending on what would make sense based on the output
6. a readme.md file summarizing what the project is and what the results were, including the bar chart.
