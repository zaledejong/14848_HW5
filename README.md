# I managed to get wordCount working in my Jupyter Notebook for MML Spark, but I ran into some syntax errors when trying to get the inverted index code to work.
# Specifically, it didn't like the format of my .flatMap when I tried running the code with .wholeTextFiles instead of .textFile. The basic idea of .wholeTextFiles is that it returns an object in the format (file name, content), and I would then split the content for each word and pass it to the mapper.
# Then, I would filter this rdd for any words that are in the stoplist before passing it on to the .map function.
#
# References
# https://stackoverflow.com/questions/47657531/pyspark-inverted-index
# https://stackoverflow.com/questions/40287237/pyspark-dataframe-operator-is-not-in
