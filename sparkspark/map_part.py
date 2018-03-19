#!/usr/bin/env python
# encoding: utf-8

"""
@description: 通过分区计算平均值

@author: pacman
@time: 2018/3/19 15:26
"""

import pyspark


def combineCouter(c1, c2):
    """
    定义 两个集群分片上面 数据结合的方式
    :param c1:
    :param c2:
    :return:
    """
    return c1[0] + c2[0], c1[1] + c2[1]


def basicAvg(nums):
    return nums.map(lambda num: (num, 1)).reduce(combineCouter)


def partitionCouter(nums):
    """
    传入迭代器，每个分片 只会被调用一次 以提升效率
    :param nums:
    :return:
    """
    sumCount = [0, 0]
    for num in nums:
        sumCount[0] += num
        sumCount[1] += 1

    return [sumCount]


def fastAvg(nums):
    sumCount = nums.mapPartitions(partitionCouter).reduce(combineCouter)
    return sumCount


def run():
    with pyspark.SparkContext('local', 'mapAndPartition') as sc:
        rdd = sc.parallelize([1, 2, 3, 5])
        # print(basicAvg(rdd))
        print(fastAvg(rdd))


def main():
    run()


if __name__ == '__main__':
    main()
