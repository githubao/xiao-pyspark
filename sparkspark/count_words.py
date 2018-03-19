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
    """
    i ---> 4
    am ---> 4
    foo ---> 2
    silly ---> 2
    stupid ---> 2
    and ---> 2
    i am foo
    :return: 
    """
    input_file = 'C:\\Users\\BaoQiang\\Desktop\\test.txt'

    with pyspark.SparkContext('local', 'wordCount') as sc:
        sc.setLogLevel("ERROR")

        # lines = sc.textFile(input_file)
        lines = sc.parallelize(['i am foo', 'i am not silly'])
        words = lines.flatMap(lambda line: line.split(' ')).map(lambda word: (word, 1))
        counts = words.reduceByKey(operator.add)
        sorted_counts = counts.sortBy(lambda x: x[1], False)
        for word, count in sorted_counts.toLocalIterator():
            print('{} ---> {}'.format(word, count))

        # 把数据存储到磁盘上的持久化操作
        lines.persist()

        foo_lines = lines.filter(lambda line: 'foo' in line)
        print(foo_lines.first())


def main():
    run()


if __name__ == '__main__':
    main()
