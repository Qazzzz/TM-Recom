__author__ = 'Qazzzz'
# -*- coding: utf-8 -*-
# -*- working on PY2 -*-

from sqlite3 import dbapi2 as sqlite
import csv
import re

Filename = 't_alibaba_data.csv'

# Cumulative days
month_to_day = {1: 0, 2: 31, 3: 59, 4: 90, 5: 120, 6: 151, 7: 181, 8: 212, 9: 243, 10: 273, 11: 304, 12: 334}


class DatabaseOpt:
    def __init__(self, dbname):
        self.con = sqlite.connect(dbname, timeout=20)

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

    def _read_csv(self):
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
