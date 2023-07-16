#!/bin/bash
start_time=$(date +%s)
echo "dummy_header, dummy_header2, dummy_header3" > dummy_data.txt
for j in {1..1000000}
do
    echo "$j,$(($j*2)),$((RANDOM))" >> dummy_data.txt
done
stop_time=$(date +%s)
elapsed_time=$((stop_time - start_time))
echo "1st block execution time: $elapsed_time seconds"
## runs in 26 seconds

## test 2; do 2 rows, increment by 2
start_time=$(date +%s)
echo "dummy_header, dummy_header2, dummy_header3" > dummy_data2.txt
for ((j=0; j<=1000000; j+=2))
do
    echo -e "$j,$(($j*2)),$((RANDOM))\n $(($j+2)),$(($(($j+2))*2)),$((RANDOM))" >> dummy_data2.txt
done
stop_time=$(date +%s)
elapsed_time=$((stop_time - start_time))
echo "2st block execution time: $elapsed_time seconds"
## runs in 17 seconds


## test 3; do 2 rows, increment by 4 -- runs in 10 seconds
start_time=$(date +%s)
echo "dummy_header, dummy_header2, dummy_header3" > dummy_data3.txt
for ((j=0; j<=1000000; j+=4))
do
    echo -e "$j,$(($j*2)),$((RANDOM))\n $(($j+2)),$(($(($j+2))*2)),$((RANDOM))\n $(($j+4)),$(($(($j+4))*2)),$((RANDOM))" >> dummy_data3.txt
done
stop_time=$(date +%s)
elapsed_time=$((stop_time - start_time))
echo "3st block execution time: $elapsed_time seconds"

## wonder if we can get it down to 5 seconds?