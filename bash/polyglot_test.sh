#!/bin/bash

### script demonstrates running multiple programming languages in bash
# to run this as an example, do ./polyglot_test.sh 100000000 4096

#assign input params
num_rows=$1
batch_size=$2

echo "Running Python Code"
# run python version
py_path="$HOME/dream_machine/python/single_thread_writer.py $num_rows $batch_size"
python3 $py_path

echo "Running Go Code"
#run go version
go_path="$HOME/dream_machine/go_code/dummy_data_writer"
cd $go_path
go_arg="parallel_v2.go -rows $num_rows -files 10"
go run $go_arg