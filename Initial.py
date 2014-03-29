__author__ = 'Qazzzz'
# -*- coding: utf-8 -*-
# -*- working on PY2 -*-

from sqlite3 import dbapi2 as sqlite
import csv
import re
import math

Filename = 't_alibaba_data.csv'

# Cumulative days
month_to_day = {1: 0, 2: 31, 3: 59, 4: 90, 5: 120, 6: 151, 7: 181, 8: 212, 9: 243, 10: 273, 11: 304, 12: 334}


class DatabaseOpt:
    def __init__(self, dbname = "tianmao"):
        self.con = sqlite.connect(dbname, timeout=20)
        self.userinfo = []
        self.userid = []
        self.brandid = []
        

    def __del__(self):
        self.con.close()

    def db_commit(self):
        self.con.commit()

    def create_index_tables(self):
        self.con.execute('''CREATE TABLE IF NOT EXISTS userinfo(user_id integer, brand_id integer,
                                                    type_id integer, visit_datetime integer)''')
        self.con.execute("DELETE FROM userinfo")
        self.con.execute("CREATE TABLE IF NOT EXISTS userid(user_id integer PRIMARY KEY )")
        self.con.execute("DELETE FROM userid")
        self.con.execute("CREATE TABLE IF NOT EXISTS brandid(brand_id integer PRIMARY KEY )")
        self.con.execute("DELETE FROM brandid")

        self._read_csv()
        print('db created and csv is imported')
        self.db_commit()

    def _read_csv(self,filename = r't_alibaba_data.csv'):
        csv_file = file(Filename, 'rb')
        reader = csv.reader(csv_file)
        chinese_filter = re.compile('\d+')
        for line in reader:
            if reader.line_num == 1:
                continue
            # Transform the 'visit_datetime'
            time = chinese_filter.findall(unicode(line[3], 'gb2312'))
            month = int(time[0])
            day = int(time[1])
            time = month_to_day[month] + day

            self.con.execute("INSERT INTO userinfo VALUES (%d, %d, %d, %d)"
                             % (int(line[0]), int(line[1]), int(line[2]), time))
            if not self.con.execute("SELECT * FROM userid WHERE user_id=%d" % int(line[0])).fetchall():
                self.con.execute("INSERT INTO userid VALUES (%d)" % int(line[0]))
            if not self.con.execute("SELECT * FROM brandid WHERE brand_id=%d" % int(line[1])).fetchall():
                self.con.execute("INSERT INTO brandid VALUES (%d)" % int(line[1]))
        csv_file.close()

    def read_all_info(self, user_id):
        return self.con.execute("SELECT * FROM userinfo WHERE user_id=%d" % user_id).fetchall()

    def read_brand_info(self, user_id, type_id):
        if type_id == 'all':
            return [brandid[0] for brandid in self.con.execute("SELECT brand_id FROM userinfo WHERE user_id=%d"
                                                               % user_id).fetchall()]
        else:
            return [brandid[0] for brandid in
                    self.con.execute("SELECT brand_id FROM userinfo WHERE user_id=%d and type_id=%d"
                                     % (user_id, type_id)).fetchall()]

    def user_num(self):
        return self.con.execute("SELECT count(1) from userid").fetchone()[0]

    def brand_num(self):
        return self.con.execute("SELECT count(1) from brandid").fetchone()[0]
    
    def get_userinfo_table(self):
        if len(self.userinfo) == 0:
            self.userinfo = self.con.execute("SELECT * FROM userinfo").fetchall()
        return self.userinfo
        
    def get_userid_table(self):
        if len(self.userid) == 0:
            self.userid = [i for (i,) in self.con.execute("SELECT * FROM userid").fetchall()]
        return self.userid
        
    def get_brandid_table(self):
        if len(self.brandid) == 0:
            self.brandid = [i for (i,) in self.con.execute("SELECT * FROM brandid").fetchall()]
        return self.brandid
        
    def memory_tables_init(self):
        self.get_userinfo_table()
        self.get_userid_table()
        self.get_brandid_table()
        
    
        
    def _test(self):
        self.memory_tables_init()
        

class Predictor:
    def __init__(self,data = ""):
        if data == "":
            self.data = DatabaseOpt()
        else:
            self.data = data
            
    def make_prefs(self):
        self.prefs = dict()
        for entry in self.data.userinfo:
            if entry[0] in self.prefs:
                self.prefs[entry[0]].update({entry[3]:self.get_score(entry[2])})
            else:
                self.prefs[entry[0]] = {entry[3]:self.get_score(entry[2])}
      
    def sim_pearson(self,p1,p2):
        si = {}
        for item in self.prefs[p1]:
            if item in self.prefs[p2]:
                si[item] = 1
        n = len(si)
        if n == 0:
            return 1
        sum1 = sum([self.prefs[p1][it] for it in si])
        sum2 = sum([self.prefs[p2][it] for it in si])
        
        sum1Sq = sum([pow(self.prefs[p1][it],2) for it in si])
        sum2Sq = sum([pow(self.prefs[p2][it],2) for it in si])
        
        pSum = sum([self.prefs[p1][it]*self.prefs[p2][it] for it in si])
        
        num = pSum - (sum1*sum2/n)
        den = math.sqrt((sum1Sq - pow(sum1,2)/n) * (sum2Sq - pow(sum2,2)/n))
        if den == 0: 
            return 0
        
        r = num/den
        return r
        
    def get_score(self,x):
        if x == 0:
            return 1
        elif x == 2:
            return 3
        elif x == 3:
            return 4
        elif x == 1:
            return 6
            
    def top_matches(self,person,n = 5,similarity = ""):
        if similarity == "":
            similarity = self.sim_pearson
        self.scores = [(similarity(person,other),other) 
                        for other in self.prefs if other != person]
                            
        self.scores.sort()
        self.scores.reverse()
        return self.scores[0:n]
        
    def get_recommendations(self,person,n = 6,similarity = ""):
        if similarity == "":
            similarity = self.sim_pearson
        totals = {}
        simSums = {}
        for other in self.prefs:
            if other == person:
                continue
            sim = similarity(person,other)
            if sim <= 0:
                continue
            for item in self.prefs[other]:
                #if item not in self.prefs[person]:
                totals.setdefault(item,0)
                totals[item] += self.prefs[other][item] * sim
                simSums.setdefault(item,0)
                simSums[item] += sim
        
        rankings = [(total/simSums[item],item) for item,total in totals.items()]
        
        rankings.sort()
        rankings.reverse()
        return rankings[0:n]
        
    def get_recommendations_list(self,n = 6,similarity = ""):
        self.recommend_list = [(i,self.get_recommendations(i,n,similarity)) for i in self.data.userid]
        
    def print_result(self):
        f = open(r"result",'w')
        for entry in self.recommend_list:
            s = str(entry[0]) + "\t"
            for bid in entry[1]:
                s += str(bid[1])
                s += ","
            s = s[0:-1] + "\n"
            f.write(s)
        f.flush()
        f.close()
        
    
    
    def _test(self):
        self.data._test()
        self.make_prefs()