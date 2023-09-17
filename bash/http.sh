#!/bin/bash

: '
    Author: Matt Martin
    Date: 9/17/23
    Desc: demonstrates web requests with bash...keeps it simple :)
    had to install wget and jq
'

file="test_res.json"
url="https://api.chucknorris.io/jokes/random"

#wget -q -O $file $url

#curl version of command:
curl -s $url -o $file

#get the joke (element "value" in the json response)
joke=$(jq -r '.value' $file)

echo $joke