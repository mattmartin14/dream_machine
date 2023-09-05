#!/bin/bash

#assign input params
#num_rows=$1
#batch_size=$2

cd

echo "Running Go Benchmark >>"
#run go version
exec_path="$DREAM_MACHINE/benchmarks/write_speed/go_lang"
cd $exec_path
./go_writer
# go_arg="parallel_v2.go -rows $num_rows -files 10"
# go run $go_arg

echo "------------------------------------------"
cd

echo "Running Rust Benchmark >>"
exec_path="$DREAM_MACHINE/benchmarks/write_speed/rust/rust/target/release"
cd $exec_path
./rust

echo "------------------------------------------"
cd

echo "running C Benchmark >>"
exec_path="$DREAM_MACHINE/benchmarks/write_speed/c_lang"
cd $exec_path
./c_writer

echo "------------------------------------------"
cd

echo "Running C++ Benchmark >>"
exec_path="$DREAM_MACHINE/benchmarks/write_speed/cpp"
cd $exec_path
./cpp_test

#echo "Running Python Code"
# run python version
#py_path="$DREAM_MACHINE/python/single_thread_writer.py $num_rows $batch_size"
#python3 $py_path
