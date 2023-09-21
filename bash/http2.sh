url="https://api.chucknorris.io/jokes/random"
joke=$(curl -s $url | jq -r '.value')
echo $joke