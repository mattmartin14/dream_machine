#!/bin/bash

### script launches the different 1B lang writers
# to run this as an example, do ./polyglot_test.sh 100000000 4096

#assign input params
#num_rows=$1
#batch_size=$2

echo "Running Go Benchmark >>"
#run go version
exec_path="$DREAM_MACHINE/benchmarks/write_speed/go_lang"
cd $exec_path
./go_lang
# go_arg="parallel_v2.go -rows $num_rows -files 10"
# go run $go_arg

cd

echo "Running Rust Benchmark >>"
exec_path="$DREAM_MACHINE/benchmarks/write_speed/rust/rust/target/release"
cd $exec_path
./rust


#echo "Running Python Code"
# run python version
#py_path="$DREAM_MACHINE/python/single_thread_writer.py $num_rows $batch_size"
#python3 $py_path

#echo "Running Go Code >>"
#run go version
#go_path="$DREAM_MACHINE/go_code/dummy_data_writer"
#echo $go_path
#cd $go_path
#go_arg="parallel_v2.go -rows $num_rows -files 10"
#go run $go_arg