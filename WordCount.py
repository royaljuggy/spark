import pyspark
import os
import re
import shutil
from operator import add


sc = pyspark.SparkContext('local[*]')
sc.setLogLevel('WARN')

txt = sc.textFile('data/sample-story.txt')

def tokenize(text):
    return re.findall(r'\b[a-z]+(?:\'[a-z]+)?\b', text.lower())

wc = txt\
	.flatMap(lambda line: tokenize(line)) \
	.map(lambda word: (word, 1)) \
	.reduceByKey(add) \
	.sortByKey()

# delete file if it already exists (spark will create file for us)
output = "output"
if os.path.exists(output):
    shutil.rmtree(output)

# save to textfile
wc.saveAsTextFile(output)
