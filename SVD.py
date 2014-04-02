# -*- coding: utf-8 -*-
# -*- working on PY2 -*-
# TODO: 外部参数文件输出函数
# TODO: 用户评价得分体系需要优化，归一？引入日期因素？
# TODO: 优化矩阵时采用什么收敛方法好？

from Initial import DatabaseOpt

import math
import random
import cPickle as Pickle

configure_file = 'svd_configure.txt'
model_save_file = 'svd_model.pkl'
result_save_file = 'result_svd.txt'
test_data_file = 'test_set.txt'

# 分解维数
factor_num = 10
learn_rate = 0.01
# 正则化系数
regularization = 0.05
# 样本集比例
sample_rate = 0.8
# 每用户输出多少个预测item
select_num = 6

SCORE_CLICK = 1
SCORE_SHOUCANG = 4
SCORE_GOUWUCHE = 6
SCORE_BUY = 8


class SVD:
    def __init__(self, dbname='', rate=sample_rate):
        if dbname is None:
            self.data = DatabaseOpt(rate=rate)
        else:
            self.data = DatabaseOpt(dbname=dbname, rate=rate)
        self.prefs = {}
        self.prefs_test = {}
        self.result = {}

        # 用户数量与item数量
        self.user_num = self.data.user_num()
        self.item_num = self.data.brand_num()
        # 全部评分的平均分
        self.average_score = 0.0
        # 全部评分中的最大分值与最小分值
        self.min_score = 1.0
        self.max_score = 0.0

    # 遍历列表中的每一个人
    # 遍历该用户的每一条商品记录，依次是用户id，行动，品牌id，日期
    # 偏好格式: {用户id:{商品1id:评价得分,商品2id:评价得分,...},...}
    def _generate_input_matrix(self):
        for usr in self.data.sample_collection:
            self.prefs[usr] = {}
            for entry in self.data.sample_collection[usr]:
                if entry[1] in self.prefs[entry[0]]:
                    self.prefs[entry[0]].update({entry[1]: get_score(entry[2]) + self.prefs[entry[0]][entry[1]]})
                else:
                    self.prefs[entry[0]].update({entry[1]: get_score(entry[2])})
        for usr in self.data.test_collection:
            self.prefs_test[usr] = {}
            for entry in self.data.test_collection[usr]:
                if entry[1] in self.prefs_test[entry[0]]:
                    self.prefs_test[entry[0]].update({entry[1]: get_score(entry[2]) + self.prefs_test[entry[0]][entry[1]]})
                else:
                    self.prefs_test[entry[0]].update({entry[1]: get_score(entry[2])})

    def _cal_metadata(self):
        count = 0
        result = 0.0
        for usr_id in self.prefs:
            for item_id in self.prefs[usr_id]:
                count += 1
                score = self.prefs[usr_id][item_id]
                if score > self.max_score:
                    self.max_score = score
                if score < self.min_score:
                    self.min_score = score
                result += score
        self.average_score = result / float(count)

    # 分数预测
    def predict_score(self, av, bu, bi, pu, qi):
        p_score = av + bu + bi + self._iner_product(pu, qi)
        if p_score < self.min_score:
            p_score = self.min_score
        elif p_score > self.max_score:
            p_score = self.max_score
        return p_score

    # 求向量v1, v2的内积
    def _iner_product(self, v1, v2):
        result = 0
        for i in range(len(v1)):
            result += v1[i] * v2[i]
        return result

    # 测试优化后的矩阵
    # 采用均方根误差(RMSE)
    def _validate(self, av, bu, bi, pu, qi, test_data=None):
        cnt = 0
        rmse = 0.0
        if test_data is not None:
            fi = open(test_data, 'r')
            for line in fi:
                cnt += 1
                arr = line.split()
                usr_id = int(arr[0].strip()) - 1
                item_id = int(arr[1].strip()) - 1
                p_score = self.predict_score(av, bu[usr_id], bi[item_id], pu[usr_id], qi[item_id])
                t_score = float(arr[2].strip())
                rmse += (t_score - p_score) * (t_score - p_score)
            fi.close()
        else:
            for usr_id in self.prefs_test:
                for item_id in self.prefs_test[usr_id]:
                    cnt += 1
                    p_score = self.predict_score(self.average_score, bu[usr_id], bi[item_id], pu[usr_id], qi[item_id])
                    t_score = self.prefs_test[usr_id][item_id]
                    rmse += (t_score - p_score) * (t_score - p_score)
        return math.sqrt(rmse / cnt)

    # bi: 第i个item的偏离程度; bu: 第u个用户的偏离程度; [均与平均评价得分相比]
    # qi: item矩阵, 规模self.itemNum x factorNum; pu: user矩阵, 规模self.userNum x factorNum
    # 总矩阵分解为 A = pu * 转置(qi)
    def svd_process(self, conf=configure_file, model_save=model_save_file):
        # calculate input and metadata
        self._generate_input_matrix()
        self._cal_metadata()

        # Initialization
        bi = {}
        bu = {}
        qi = {}
        pu = {}
        temp = math.sqrt(factor_num)
        for usr_id in self.data.userid:
            bu.setdefault(usr_id, 0.0)
            pu.setdefault(usr_id, [(0.1 * random.random() / temp) for j in range(factor_num)])
        for item_id in self.data.brandid:
            bi.setdefault(item_id, 0.0)
            qi.setdefault(item_id, [(0.1 * random.random() / temp) for j in range(factor_num)])
        print("initialization end\nstart training\n")

        # train model
        pre_rmse = 1000000.0
        for step in range(100):
            print("Iterating %d" % step)
            for usr_id in self.prefs:
                for item_id in self.prefs[usr_id]:
                    score = self.prefs[usr_id][item_id]
                    prediction = self.predict_score(self.average_score, bu[usr_id], bi[item_id], pu[usr_id], qi[item_id])
                    eui = score - prediction

                    #update parameters
                    bu[usr_id] += learn_rate * (eui - regularization * bu[usr_id])
                    bi[item_id] += learn_rate * (eui - regularization * bi[item_id])
                    for k in range(factor_num):
                        temp = pu[usr_id][k]
                        pu[usr_id][k] += learn_rate * (eui * qi[item_id][k] - regularization * pu[usr_id][k])
                        qi[item_id][k] += learn_rate * (eui * temp - regularization * qi[item_id][k])

            #learnRate *= 0.9
            cur_rmse = self._validate(self.average_score, bu, bi, pu, qi)
            print("test_RMSE in step %d: %f" % (step, cur_rmse))
            if cur_rmse >= pre_rmse:
                break
            else:
                pre_rmse = cur_rmse

        # save the model
        fo = file(model_save, 'wb')
        Pickle.dump(bu, fo, True)
        Pickle.dump(bi, fo, True)
        Pickle.dump(qi, fo, True)
        Pickle.dump(pu, fo, True)
        fo.close()
        print("model generation over")

    def predict(self, model=model_save_file):
        # get the model
        fi = file(model, 'rb')
        bu = Pickle.load(fi)
        bi = Pickle.load(fi)
        qi = Pickle.load(fi)
        pu = Pickle.load(fi)
        fi.close()

        # predict
        # self.result format: {user_id 1:[(brand_id 1, p_score), (brand_id 2, p_score), ...], user_id 2: [...], ...}
        print("Waiting for predicting")
        for usr_id in self.data.userid:
            self.result[usr_id] = []
            for item_id in self.data.brandid:
                p_score = self.predict_score(self.average_score, bu[usr_id], bi[item_id], pu[usr_id], qi[item_id])
                self.result[usr_id].append((item_id, p_score))
            self.result[usr_id].sort(key=lambda x: x[1], reverse=True)

        # save result
        self._save_result()

    def _save_result(self, n=select_num, result=result_save_file):
        fo = open(result, 'w')
        for usr_id in self.result:
            if n > len(self.result[usr_id]):
                n = len(self.result[usr_id])
            s = str(usr_id) + "\t"
            for item_id in self.result[usr_id][0:n-1]:
                s += str(item_id[0]) + ","
            s = s[0:-1] + "\n"
            fo.write(s)
        fo.flush()
        fo.close()
        print("Result has been output")


def get_score(x):
    if x == 0:
        return SCORE_CLICK
    elif x == 2:
        return SCORE_SHOUCANG
    elif x == 3:
        return SCORE_GOUWUCHE
    elif x == 1:
        return SCORE_BUY