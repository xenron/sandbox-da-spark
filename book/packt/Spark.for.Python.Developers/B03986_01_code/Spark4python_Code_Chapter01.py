#
# Spark for Python Developers
# Chapter 1 - Code Word Count
#

# Word count on manuscript using PySpark

# import regex module
import re
# import add from operator module
from operator import add


# read input file
file_in = sc.textFile('/home/an/Documents/A00_Documents/Spark4Py 20150315')

# count lines
print('number of lines in file: %s' % file_in.count())

# add up lenths of each line
#
chars = file_in.map(lambda s: len(s)).reduce(add)
print('number of characters in file: %s' % chars)

# Get words from the input file
words =file_in.flatMap(lambda line: re.split('\W+', line.lower().strip()))

# words of more than 3 characters
words = words.filter(lambda x: len(x) > 3)

# set count 1 per word
words = words.map(lambda w: (w,1))

# reduce phase - sum count all the words
words = words.reduceByKey(add)

# create tuple (count, word) and sort in descending
words = words.map(lambda x: (x[1], x[0])).sortByKey(False)

# take top 20 words by frequency
words.take(20)

# create function for hitogram of most frequent words
#

% matplotlib inline
import matplotlib.pyplot as plt
#

def histogram(words):
    count = map(lambda x: x[1], words)
    word = map(lambda x: x[0], words)
    plt.barh(range(len(count)), count,color = 'grey')
    plt.yticks(range(len(count)), word)

# Change order of tuple (word, count) from (count, word) 
words = words.map(lambda x:(x[1], x[0]))
words.take(25)

# display histogram
histogram(words.take(25))

# words in one summarised statement
words = sc.textFile('/home/an/Documents/A00_Documents/Spark4Py 20150315')
        .flatMap(lambda line: re.split('\W+', line.lower().strip()))
        .filter(lambda x: len(x) > 3)
        .map(lambda w: (w,1))
        .reduceByKey(add)
        .map(lambda x: (x[1], x[0])).sortByKey(False)
words.take(20)