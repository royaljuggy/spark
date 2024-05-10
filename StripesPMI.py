import pyspark
import os
import math
import re
import shutil
import sys
from Parser import *

def tokenize(text):
    return re.findall(r'\b[a-z]+(?:\'[a-z]+)?\b', text.lower())

def main():
    # ARGUMENTS
    # 0 = program name
    # 1 = output file
    # 2 = input file
    output = get_output_dir()

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
        bgs = dict()
        for pair in pairsToEmit:
            l.append((pair[0], {pair[1] : 1}))
        return l

    def combineMaps(tup1, tup2):
        # tup is of type (Map(String, Int))
        # We update tup1 in-place by adding all pairs from tup2 to tup1
        for k, v in tup2.items():
            if k in tup1:
                # update
                tup1[k] += v
            else:
                tup1[k] = v

        return tup1

    def calculatePMI(tup):
        key_lhs = tup[0]
        new_dict = dict()

        for key_rhs, count in tup[1].items():
            lc = lines.value
            p_of_AB = count / lc
            p_of_A = boc.value[key_lhs] / lc
            p_of_B = boc.value[key_rhs] / lc
            den = p_of_A * p_of_B
            PMI = math.log10(p_of_AB / den)
            new_dict[key_rhs] = (PMI, count)

        return (key_lhs, new_dict)
        # def pmi(tup_rhs):
        #     key_rhs = tup_rhs[0]
        #     count = tup_rhs[1]
        #     lc = lines.value
        #     p_of_AB = count / lc
        #     p_of_A = boc.value[key_lhs] / lc
        #     p_of_B = boc.value[key_rhs] / lc
        #     den = p_of_A * p_of_B
        #     PMI = math.log10(p_of_AB / den)

        #     return (key_rhs, (PMI, count))

        # return (key_lhs, map(pmi, tup[1]))
    
    # job two
    bigrams = txt \
        .flatMap(bigrams) \
        .reduceByKey(combineMaps) \
        .map(lambda tup: (tup[0], dict(filter(lambda pair: pair[1] >= threshold, tup[1].items())))) \
        .map(calculatePMI) \
        .filter(lambda tup: len(tup[1]) > 0) \
        .sortBy(lambda pair: pair[0], ascending=True) # sortby key {k:v for (k,v) in tup[1].items() if v >= threshold}
    
    # TODO: increase efficiency. Try to use only one filter if possible. Second filter removes empty tuples

    # OUTPUT...
    # delete file if it already exists (spark will create file for us)
    if os.path.exists(output):
        shutil.rmtree(output)

    # save to textfile
    bigrams.saveAsTextFile(output)

if __name__ == "__main__":
    main()