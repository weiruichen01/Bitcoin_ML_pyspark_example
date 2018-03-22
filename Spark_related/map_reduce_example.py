df = spark.read.load("/home/ac/Desktop/map_reduce_data.csv",format="csv", delimiter=",", header=True)

df.show() #print out the dataset

df.select().show()

tmp = df.select('value').rdd.map(lambda x: 1)  #map any value to 1
count = tmp.reduce(lambda x,y:x + y) 

#this demo aims to show the power of Apache Spark
#use htop or top to see that CPU cores run to 100% when performing the tasks
