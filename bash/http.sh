#!/bin/bash

: '
    Author: Matt Martin
    Date: 9/17/23
    Desc: demonstrates web requests with bash...keeps it simple :)
    had to install wget and jq
'

#file="test_res.json"
#curl -s $url -o $file
#joke=$(jq -r '.value' $file)
#echo $joke

#this just outputs the one value we want to a variable all in one line
#uses a combo of curl piping results to jq to parse

url="https://api.chucknorris.io/jokes/random"
joke=$(curl -s $url | jq -r '.value')
echo $joke
echo "--------"
#this line just prints out the joke (no variable assignment needed)
curl -s $url | jq -r '.value'
#and this version writes the joke to a file
echo "--------"
curl -s $url | jq -r '.value' > joke.txt
cat joke.txt