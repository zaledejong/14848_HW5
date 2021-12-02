import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import input_file_name

conf = SparkConf()
# create Spark context with necessary configuration
sc = SparkContext.getOrCreate(conf=conf)

stoplist = ["they", "she", "he", "it", "the", "as", "is", "and"]

# Conduct MapReduce and write the output to folder
invertedIndex = sc.wholeTextFiles("/notebooks/data/").flatMap(lambda (file, content):[(file, word) for word in content.split(" ")])
invertedIndex.filter(~invertedIndex.word.isin(stoplist))
invertedIndex.map(lambda (file, word): (word,[file], 1)).reduceByKey(lambda a,b: a+b).saveAsTextFile("/notebooks/output")
