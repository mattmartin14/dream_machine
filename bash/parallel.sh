#!/bin/bash
total=100000
file_cnt=50

# simple bash script to generate files of data in parallel; could probably enhance to add more than just numbers
for i in $(seq 1 $file_cnt); do
  parallel -j 1 "for ((j=1; j<=$total; j++)); do echo \$((({} - 1) * $total + j)); done > ~/test_dummy_data/bash/integers_{}.txt" ::: $i &
done

wait

total_generated=$((total * file_cnt))
echo "Total rows generated: $(printf "%'d" $total_generated)"








