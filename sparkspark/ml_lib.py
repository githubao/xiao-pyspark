#!/usr/bin/env python
# encoding: utf-8

"""
@description: spark的机器学习

@author: pacman
@time: 2018/3/19 18:22
"""

import pyspark
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.classification import LogisticRegressionWithSGD

"""
1. 特征提取
常用的技术：TF-IDF，word2vec
常用的优化手段：缩放，正则化

2. 统计
最大值，最小值，平均值，方差

3. 分类和回归
分类：预测垃圾邮件
回归：给定身高预测体重

线性回归，逻辑回归，支持向量机，朴素贝叶斯，决策树和随机森林

4. 聚类
K-means

5. 协同过滤和推荐

6. 降维
主成分分析 奇异值分解

一些机器学习的最佳实践：
1. 正确提取特征
2. 特征使用之前需要正则化
3. 配置合理的算法参数
4. 使用稀疏矩阵
5. 使用并行算法

"""


def run():
    with pyspark.SparkContext('local', 'mapAndPartition') as sc:
        spam = sc.textFile('spam.txt')
        normal = sc.textFile('normal.txt')

        htf = HashingTF(numFeatures=10000)
        spamFeatures = spam.map(lambda email: htf.transform(email.split(' ')))
        normalFeatures = normal.map(lambda email: htf.transform(email.split(' ')))

        positiveExamples = spamFeatures.map(lambda features: LabeledPoint(1, features))
        negativeExamples = normalFeatures.map(lambda features: LabeledPoint(0, features))

        trainingData = positiveExamples.union(negativeExamples)
        trainingData.cache()

        model = LogisticRegressionWithSGD.train(trainingData)

        posTest = htf.transform('D M G GET cheap stuff by sending money to ...'.split(' '))
        negTest = htf.transform('Hi Dad, I started studying Spark the other ...'.split(' '))

        print('Prediction for positive test example: {}'.format(model.predict(posTest)))
        print('Prediction for negative test example: {}'.format(model.predict(negTest)))


def main():
    run()


if __name__ == '__main__':
    main()
