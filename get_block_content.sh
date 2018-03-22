for i in $(seq 500000 513357)
do
        bitcoin-cli getblock $(bitcoin-cli getblockhash "$i") > ~/block_data/$i
done
