#get back the content from block no.500000 to block no.513357
#modify the number to get different range of block data
for i in $(seq 500000 513357)
do
        bitcoin-cli getblock $(bitcoin-cli getblockhash "$i") > ~/block_data/$i
done


