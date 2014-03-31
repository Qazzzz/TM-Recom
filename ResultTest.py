# -*- coding: utf-8 -*-
# -*- working on PY2 -*-

import re
import math


# Calculate the F1 score
# Data is read from 2 files which contain prediction result and test set
class ResultTest:
    def __init__(self, result_file='result.txt', test_set_file='test_set.txt'):
        self.result = {}
        self.test_set = {}
        self.precision = 0
        self.recall = 0
        self.f1 = 0
        self._read_file(result_file, self.result)
        self._read_file(test_set_file, self.test_set)
        self._calculate_f1_score()

    def _read_file(self, filename, target):
        content_filter = re.compile('\d+')
        f = open(filename)
        for line in f:
            temp = [int(item) for item in content_filter.findall(line)]
            if len(temp) == 1:
                continue
            target.setdefault(temp[0], 0)
            target[temp[0]] = temp[1:]
        f.flush()
        f.close()

    def _calculate_precision(self):
        hitBrands = 0
        pBrands = 0
        for i in range(len(self.test_set)):
            temp_test = self.test_set[self.test_set.keys()[i]]
            temp_pred = self.result[self.test_set.keys()[i]]
            temp = [item for item in temp_test if item in temp_pred]
            hitBrands += len(temp)
            pBrands += len(temp_pred)
        self.precision = hitBrands/pBrands

    def _calculate_recall(self):
        hitBrands = 0
        bBrands = 0
        for i in range(len(self.test_set)):
            temp_test = self.test_set[self.test_set.keys()[i]]
            temp_pred = self.result[self.test_set.keys()[i]]
            temp = [item for item in temp_test if item in temp_pred]
            hitBrands += len(temp)
            bBrands += len(temp_test)
        self.recall = hitBrands/bBrands

    def _calculate_f1_score(self):
        self._calculate_precision()
        self._calculate_recall()
        if self.precision + self.recall:
            self.f1 = (2 * pow(self.precision, 2))/(self.precision + self.recall)

    def get_precision(self):
        return self.precision

    def get_recall(self):
        return self.recall

    def get_f1_score(self):
        return self.f1
