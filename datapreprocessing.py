from pyspark import SparkContext
sc = SparkContext('local[*]', 'bitcoin_map_reduce') #a sparkcontext is needed

#this program uses python pandas and spark function in a mixed fashion
#pure spark is recommended for the performance reason

#feel free to read in data with function you are familiar with. this example uses python pandas
#df = ...

#data is taken back from bigquery and looks like below
# timestamp,transactions_outputs_output_satoshis,difficultyTarget
# 1510448575000,1811862770,402705995
# 1510448575000,0,402705995
# 1510448575000,100000000,402705995
# 1510448575000,96690000,402705995
# 1510448575000,1000000000,402705995
# there are mulitple row sharing one timestamp
# when it loops to a new timestamp, create another key-value pair in dictionary

timestamp_current_block = df.iloc[0,0]
Dict = {}
#four lines of code below are for the first block
Dict[timestamp_current_block] = {}
Dict[timestamp_current_block]['output_satoshis_s'] = []
Dict[timestamp_current_block]['tx_ID'] = []
Dict[timestamp_current_block]['difficulties'] = df.iloc[0,3]

for i in range(len(df)):
	if df.iloc[i, 0] == timestamp_current_block: #same block
		Dict[timestamp_current_block]['tx_ID'].append(df.iloc[i, 1])
		Dict[timestamp_current_block]['output_satoshis_s'].append(df.iloc[i, 2]) 
	else:  #different block starts
		timestamp_current_block = df.iloc[i,0]
		Dict[timestamp_current_block] = {}
		Dict[timestamp_current_block]['output_satoshis_s'] = []
		Dict[timestamp_current_block]['tx_ID'] = []
		Dict[timestamp_current_block]['tx_ID'].append(df.iloc[i, 1])
		Dict[timestamp_current_block]['output_satoshis_s'].append(df.iloc[i, 2])
		Dict[timestamp_current_block]['difficulties'] = df.iloc[i,3]

#print(Dict)

for key, value in Dict.items():
	#key is timestamp
	#value is a dictionary with two keys "output_satoshis_s" and "diffculties"
	satoshi_rdd = sc.parallelize(value['output_satoshis_s']) 
	BTC_rdd = satoshi_rdd.map(lambda x:x/100000000) #change the unit from satoshi to BTC
	sum_BTC_of_a_block = BTC_rdd.reduce(lambda x,y:x+y) #sum up the whole RDD list
	Dict[key]['sum_BTC_of_a_block'] = sum_BTC_of_a_block #add a new key-value pair where key is timestamp and value is sum_BTC_of_a_block
	
	#calculate the number of transaction in a block
	tx_ID_rdd = sc.parallelize(value['tx_ID'])
	tx_ID_count_rdd = tx_ID_rdd.map(lambda x: 1) #map each transaction ID to 1
	num_tx_of_a_block = tx_ID_count_rdd.reduce(lambda x,y: x+y) #sum up all the ones
	Dict[key]['num_tx_of_a_block'] = num_tx_of_a_block
	

#turn Dict into csv
list_timestamps = []
list_difficulties = []
list_sum_BTC_of_a_block = []
list_num_tx_of_a_block = []
for key, value in Dict.items():
	list_timestamps.append(key)
	list_difficulties.append(value['difficulties'])
	list_num_tx_of_a_block.append(value['num_tx_of_a_block'])
	list_sum_BTC_of_a_block.append(value['sum_BTC_of_a_block'])


#calculate the difference of output_satoshi at period t+1 and t
#after calculating the difference, if difference > 0 then rise, otherwise fall.
import numpy as np
arr_BTC_of_a_block_diff = np.array(list_sum_BTC_of_a_block[1:]) - np.array(list_sum_BTC_of_a_block[0:-1]) #value of period t+1 BTC minus period t BTCarr_BTC_of_a_block_diff =  np.concatenate([[0], arr_BTC_of_a_block_diff]) #insert 0 as the difference for the first period
arr_BTC_of_a_block_diff = np.concatenate([[0], arr_BTC_of_a_block_diff]) #
list_output_BTC_rise = list(map(lambda x: x >0, arr_BTC_of_a_block_diff)) #lambda x: x > 0 will return boolean value indicating whether it rises or not


#select feature in Dict and build a dataframe to store as csv format
df_Dict = pd.DataFrame() 
df_Dict['timestamp'] = list_timestamps
df_Dict['difficulties'] = list_difficulties
df_Dict['sum_BTC_of_a_block'] = list_sum_BTC_of_a_block
df_Dict['num_tx_of_a_block'] = list_num_tx_of_a_block
df_Dict['output_BTC_rise'] = list_output_BTC_rise


#print(df_Dict.head(10))
