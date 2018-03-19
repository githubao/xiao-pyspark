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
