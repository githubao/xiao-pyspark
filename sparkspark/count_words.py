#!/usr/bin/env python
# encoding: utf-8

"""
@description: 统计词频

@author: pacman
@time: 2017/10/25 12:20
"""

import pyspark
import operator


def run():
    input_file = 'C:\\Users\\BaoQiang\\Desktop\\test.txt'

    with pyspark.SparkContext('local', 'wordCount') as sc:
        lines = sc.textFile(input_file)
        words = lines.flatMap(lambda line: line.split(' ')).map(lambda word: (word, 1))
        counts = words.reduceByKey(operator.add)
        sorted_counts = counts.sortBy(lambda x: x[1], False)
        for word, count in sorted_counts.toLocalIterator():
            print('{} ---> {}'.format(word, count))

        foo_lines = lines.filter(lambda line:'foo' in line)
        print(foo_lines.first())


def main():
    run()


if __name__ == '__main__':
    main()
