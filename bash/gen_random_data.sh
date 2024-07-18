#!/bin/bash
output_file="rand_data.txt"
> $output_file

num_rows=100000

# Write the headers to the file
echo -e "Date\tGUID" > $output_file

#rand date
generate_random_date() {
  year=$((RANDOM % 50 + 1970))
  month=$((RANDOM % 12 + 1))
  day=$((RANDOM % 28 + 1))
  printf "%04d-%02d-%02d" $year $month $day
}

for ((i=0; i<$num_rows; i++)); do
  random_date=$(generate_random_date)
  random_guid=$(uuidgen)
  echo -e "$random_date\t$random_guid" >> $output_file
done

echo "Random dates and GUIDs written to $output_file"

