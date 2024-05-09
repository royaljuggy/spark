import pyspark
import os
import math
import re
import shutil

def tokenize(text):
    return re.findall(r'\b[a-z]+(?:\'[a-z]+)?\b', text.lower())

def main():
    sc = pyspark.SparkContext('local[*]')

    # values
    WORD_LIMIT = 40
    threshold = 5 # words need to appear at least 5 times
    lineCount = sc.accumulator(0)

    txt = sc.textFile('data/cranford.txt')

    # job one, word count by line occurrence
    def tokenizeAndCount(line):
        lineCount.add(1)
        return map(lambda word: (word, 1), list(set(tokenize(line))))
        # return map(lambda word: (word, 1), list(set(tokenize(line)[:WORD_LIMIT])))

    wc = txt\
        .flatMap(tokenizeAndCount) \
        .reduceByKey(lambda x,y: x + y) \
        .filter(lambda tup: tup[1] >= threshold) \
        .coalesce(1) \
        .collectAsMap()
    
    boc = sc.broadcast(wc)

    # job two
    lines = sc.broadcast(lineCount.value)

    def bigrams(line):
        tokens = tokenize(line)
        size = len(tokens)
        if size > WORD_LIMIT:
            size = WORD_LIMIT
        pairsToEmit = set()

        for i in range(size):
            for j in range(size):
                if tokens[i] != tokens[j]:
                    pairsToEmit.add((tokens[i], tokens[j]))
        l = []
        for pair in pairsToEmit:
            l.append((pair, 1))
        return l

    def calculatePMI(tup):
        lc = lines.value
        count = tup[1]
        p_of_AB = count / lc
        p_of_A = boc.value[tup[0][0]] / lc
        p_of_B = boc.value[tup[0][1]] / lc
        den = p_of_A * p_of_B
        PMI = math.log10(p_of_AB / den)

        return (tup[0], (PMI, count))

    # job two
    bigrams = txt \
        .flatMap(bigrams) \
        .reduceByKey(lambda x,y: x + y) \
        .filter(lambda tup: tup[1] >= threshold) \
        .map(calculatePMI) \
        .sortBy(lambda pair: pair[1][0], ascending=False) # sortby highest PMI

    # OUTPUT...
    # delete file if it already exists (spark will create file for us)
    output = "output"
    if os.path.exists(output):
        shutil.rmtree(output)

    # save to textfile
    bigrams.saveAsTextFile(output)

if __name__ == "__main__":
    main()