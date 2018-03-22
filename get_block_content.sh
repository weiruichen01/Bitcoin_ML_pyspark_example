#get back the content from block no.500000 to block no.513357
#modify the number to get different range of block data
for i in $(seq 500000 513357)
do
        bitcoin-cli getblock $(bitcoin-cli getblockhash "$i") > ~/<some_path>/$i
done

#getblockhash
#input: block height
#output: block hash

#getblock
#input: block hash
#output: block content

# > will redirect data from left hand side to a text file
# put file name on the right hand side of >
