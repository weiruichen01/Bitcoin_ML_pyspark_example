#pyspark
from pyspark import SparkContext
sc = SparkContext()

# feel free to read in data with function you are familiar with. this example uses pyspark dataframe

#df = ...

import pyspark.sql.functions as f
df = df.withColumn('index', f.monotonically_increasing_id())
#this will add one column at the rightmost and serve as the index, the index will be 1,2,3 ...incrementing

num_of_row = df.count()
print(num_of_row)
training_size = num_of_row * 0.7 
training_data = df.filter(df['index'] < training_size) #first 70%
testing_data = df.filter(df['index'] > training_size) #last 30%


from pyspark.mllib.regression import LabeledPoint #since RandomForest classifier accepts not Dataframe but RDD of Labelpoints
#split features into label and predictors, and convert them into RDD of labeledpoint data type
rdd_training_data = training_data.rdd.map(lambda line: LabeledPoint( line['output_BTC_rise'], [ line[1:4] ] ) )
rdd_testing_data = testing_data.rdd.map(lambda line: LabeledPoint( line['output_BTC_rise'], [ line[1:4] ] ) )



from pyspark.mllib.tree import RandomForest, RandomForestModel
#hyperparameters are modifiable
model = RandomForest.trainClassifier(rdd_training_data, numClasses=2, categoricalFeaturesInfo={},
                                     numTrees=100, featureSubsetStrategy="auto",
                                     impurity='entropy', maxDepth=30, maxBins=32)

predictions = model.predict(rdd_testing_data.map(lambda x: x.features)) 
labelsAndPredictions = rdd_testing_data.map(lambda x: x.label).zip(predictions) 

#calculating accuracy
acc = labelsAndPredictions.filter(
    lambda lp: lp[0] == lp[1]).count() / float(rdd_testing_data.count())
print("==================================")
print('Accuracy= ' + str(acc)) 
print("==================================")                                
