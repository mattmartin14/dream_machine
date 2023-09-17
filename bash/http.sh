#!/bin/bash

: '
    Author: Matt Martin
    Date: 9/17/23
    Desc: demonstrates web requests with bash...keeps it simple :)
    had to install wget and jq
'


url="https://api.chucknorris.io/jokes/random"

#wget version
#wget -q -O $file $url

#you can output the results to a file
#get the joke (element "value" in the json response)
#file="test_res.json"
#curl -s $url -o $file
#joke=$(jq -r '.value' $file)
#echo $joke

#this just outputs the one value we want to a variable all in one line
#uses a combo of curl piping results to jq to parse
joke=$(curl -s $url | jq -r '.value')
echo $joke

