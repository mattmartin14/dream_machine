echo "row_id" > data.txt

row_cnt=10000

for ((i=1; i<=$row_cnt;i++)); do
    echo "$i" >> data.txt
done