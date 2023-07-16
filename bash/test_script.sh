#!/bin/bash

name="Matt"
echo "Hello $name"

## creating a file with data
echo "FIRST_NAME,LAST_NAME,AGE" > people.txt

## >> arrows append
echo "John,Doe,22" >> people.txt
echo "Jane,Doe,21" >> people.txt
echo "Matt,Doe,30" >> people.txt

cat people.txt

for i in {1..3}
do
    echo "Number: $i"
done

## using awk to read the files
## awk starts at base 1
## to specify a delimiter, use the -F flag
awk -F ',' '{print $2}' people.txt

## create a file with 10k lines, 3 columns, some multiplied and random values
echo "dummy_header, dummy_header2, dummy_header3" > dummy_data.txt
for ((j=0; j<=10; j+=2))
do
    echo "$j,$j,$((RANDOM))" >> dummy_data.txt
done
echo ${BASH_VERSION}

## future idea
## make bash script to generate a 1M row text file 
## then use duck db to analyze and write out to another file