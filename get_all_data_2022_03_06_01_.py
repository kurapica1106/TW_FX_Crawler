import bs4
import csv
import datetime
import io
import json
import os
import pandas
import requests
import sqlite3
import time

#商品代號: TX, MTX
def future_daily(y, m, d, dn, commodity):
    url = "https://www.taifex.com.tw/cht/3/futDailyMarketReport"
    m_code = 0 if dn.lower() == "d" else 1
    c_id = commodity.upper()
    q_date = str(y) + "/" + str(m) + "/" + str(d)
    payload = {
            "marketCode": m_code,
            "commodity_id": c_id,
            "queryDate": q_date
            }
    response = requests.get(url, params = payload)
    soup = bs4.BeautifulSoup(response.text, "lxml")
    tables = soup.select("table")
    #確認當日是否有資料
    t_check = pandas.read_html(tables[2].prettify())
    dfs_check = t_check[0]
    dfs = []
    if dfs_check.iat[1, 0] != "查無資料":
        table = pandas.read_html(tables[4].prettify())
        data_frames = table[0]
        ymd = y * 10000 + m * 100 + d
        for i in range(len(data_frames)-1):
            c_y = int(str(data_frames.iat[i, 1])[0:4])
            c_m = int(str(data_frames.iat[i, 1])[4:6])
            if len(str(data_frames.iat[i, 1])) == 8:
                if str(data_frames.iat[i, 1])[6].lower() == "w":
                    c_w = int(str(data_frames.iat[i, 1])[7])
                else:
                    c_w = 3
            else:
                c_w = 3
            c_ymw = c_y * 1000 + c_m * 10 + c_w
            _id = "%d_%s_%d" % (ymd, dn.upper(), c_ymw)
            o = int(data_frames.iat[i, 2]) if data_frames.iat[i, 2] != "-" else int()
            h = int(data_frames.iat[i, 3]) if data_frames.iat[i, 3] != "-" else int()
            l = int(data_frames.iat[i, 4]) if data_frames.iat[i, 4] != "-" else int()
            c = int(data_frames.iat[i, 5]) if data_frames.iat[i, 5] != "-" else int()
            settlement_p = data_frames.iat[i, 11] if dn.lower() == "d" else int()
            settlement_p = int(settlement_p) if settlement_p != "-" else int()
            v = data_frames.iat[i, 9] if dn.lower() == "d" else data_frames.iat[i, 8]
            v = int(v) if v != "-" else int()
            oi = data_frames.iat[i, 12] if dn.lower() == "d" else int()
            oi = int(oi) if oi != "-" else int()
            df = [_id, ymd, y, m, d, dn.upper(), c_ymw, c_y, c_m, c_w, o, h, l, c, settlement_p, v, oi]
            dfs.append(df)
    return dfs

#商品代號: TXO
def option_daily(y, m, d, dn, commodity):
    url = "https://www.taifex.com.tw/cht/3/optDailyMarketReport"
    m_code = 0 if dn.lower() == "d" else 1
    c_id = commodity.upper()
    q_date = str(y) + "/" + str(m) + "/" + str(d)
    payload = {
            "marketCode": m_code,
            "commodity_id": c_id,
            "queryDate": q_date
            }
    response = requests.get(url, params = payload)
    soup = bs4.BeautifulSoup(response.text, "lxml")
    tables = soup.select("table")
    #確認當日是否有資料
    t_check = pandas.read_html(tables[2].prettify())
    dfs_check = t_check[0]
    dfs = []
    if dfs_check.iat[1, 0] != "查無資料":
        table = pandas.read_html(tables[4].prettify())
        data_frames = table[0]
        ymd = y * 10000 + m * 100 + d
        for i in range(len(data_frames)-2):
            c_y = int(str(data_frames.iat[i, 1])[0:4])
            c_m = int(str(data_frames.iat[i, 1])[4:6])
            if len(str(data_frames.iat[i, 1])) == 8:
                if str(data_frames.iat[i, 1])[6].lower() == "w":
                    c_w = int(str(data_frames.iat[i, 1])[7])
                else:
                    c_w = 3
            else:
                c_w = 3
            c_ymw = c_y * 1000 + c_m * 10 + c_w
            strike_p = int(data_frames.iat[i, 2])
            cp = "C" if data_frames.iat[i, 3] == "Call" else "P"
            _id = "%d_%s_%s_%d_%d" % (ymd, dn.upper(), cp, c_ymw, strike_p)
            o = float(data_frames.iat[i, 4]) if data_frames.iat[i, 4] != "-" else float()
            h = float(data_frames.iat[i, 5]) if data_frames.iat[i, 5] != "-" else float()
            l = float(data_frames.iat[i, 6]) if data_frames.iat[i, 6] != "-" else float()
            c = float(data_frames.iat[i, 7]) if data_frames.iat[i, 7] != "-" else float()
            settlement_p = data_frames.iat[i, 8] if dn.lower() == "d" else float()
            settlement_p = float(settlement_p) if settlement_p != "-" else float()
            v = data_frames.iat[i, 12] if dn.lower() == "d" else data_frames.iat[i, 11]
            v = int(v) if v != "-" else int()
            oi = data_frames.iat[i, 14] if dn.lower() == "d" else int()
            oi = int(oi) if oi != "-" else int()
            df = [_id, ymd, y, m, d, dn.upper(), cp, c_ymw, c_y, c_m, c_w, strike_p, o, h, l, c, settlement_p, v, oi]
            dfs.append(df)
    return dfs

#商品代號: TXF MXF
def future_3m(y, m, d, commodity):
    url = "https://www.taifex.com.tw/cht/3/futContractsDate"
    c_id = commodity.upper()
    q_date = str(y) + "/" + str(m) + "/" + str(d)
    payload = {
            "commodityId": c_id,
            "queryDate": q_date
            }
    response = requests.get(url, params = payload)
    soup = bs4.BeautifulSoup(response.text, "lxml")
    tables = soup.select("table")
    #確認當日是否有資料
    t_check = pandas.read_html(tables[2].prettify())
    dfs_check = t_check[0]
    dfs = []
    if dfs_check.iat[0, 1] != "查無資料":
        table = pandas.read_html(tables[3].prettify())
        data_frames = table[0]
        ymd = y * 10000 + m * 100 + d
        for i in range(3, 6):
            id_3m = "自營" if data_frames.iat[i, 2] == "自營商" else data_frames.iat[i, 2]
            bs = "B"
            _id = "%d_%s_%s" % (ymd, id_3m, bs)
            v , v_amt , oi , oi_amt = \
                int(data_frames.iat[i, 3]), int(data_frames.iat[i, 4]), \
                int(data_frames.iat[i, 9]), int(data_frames.iat[i, 10])
            df = [_id, ymd, y, m, d, id_3m, bs, v, float(), v_amt, oi, float(), oi_amt]
            dfs.append(df)
            bs = "S"
            _id = "%d_%s_%s" % (ymd, id_3m, bs)
            v , v_amt , oi , oi_amt = \
                int(data_frames.iat[i, 5]), int(data_frames.iat[i, 6]), \
                int(data_frames.iat[i, 11]), int(data_frames.iat[i, 12])
            df = [_id, ymd, y, m, d, id_3m, bs, v, float(), v_amt, oi, float(), oi_amt]
            dfs.append(df)
    return dfs

#商品代號: TXO
def option_3m(y, m, d, commodity):
    url = "https://www.taifex.com.tw/cht/3/callsAndPutsDate"
    c_id = commodity.upper()
    q_date = str(y) + "/" + str(m) + "/" + str(d)
    payload = {
            "queryType": 1,
            "doQuery": 1,
            "commodityId": c_id,
            "queryDate": q_date
            }
    response = requests.get(url, params = payload)
    soup = bs4.BeautifulSoup(response.text, "lxml")
    tables = soup.select("table")
    #確認當日是否有資料
    t_check = pandas.read_html(tables[2].prettify())
    dfs = []
    if len(t_check) > 0:
        dfs_check = t_check[0]
        if dfs_check.iat[0, 1] != "查無資料":
            table = pandas.read_html(tables[3].prettify())
            data_frames = table[0]
            ymd = y * 10000 + m * 100 + d
            for i in range(3, 9):
                id_3m = "自營" if data_frames.iat[i, 3] == "自營商" else data_frames.iat[i, 3]
                cp = "C" if data_frames.iat[i, 2] == "買權" else "P"
                bs = "B"
                _id = "%d_%s_%s_%s" % (ymd, id_3m, cp, bs)
                v , v_amt , oi , oi_amt = \
                    int(data_frames.iat[i, 4]), int(data_frames.iat[i, 5]), \
                    int(data_frames.iat[i, 10]), int(data_frames.iat[i, 11])
                df = [_id, ymd, y, m, d, id_3m, cp, bs, v, float(), v_amt, float(), oi, float(), oi_amt, float()]
                dfs.append(df)
                bs = "S"
                _id = "%d_%s_%s_%s" % (ymd, id_3m, cp, bs)
                v , v_amt , oi , oi_amt = \
                    int(data_frames.iat[i, 6]), int(data_frames.iat[i, 7]), \
                    int(data_frames.iat[i, 12]), int(data_frames.iat[i, 13])
                df = [_id, ymd, y, m, d, id_3m, cp, bs, v, float(), v_amt, float(), oi, float(), oi_amt, float()]
                dfs.append(df)
    return dfs

#fu_op: Future, Option
#info_type: daily, 3m
def load_data(fu_op, commodity, info_type, y1, m1, d1, y2, m2, d2):
    fu_op = fu_op.lower()
    info_type = info_type.lower()
    
    if (y1, m1, d1) < (y2, m2, d2):
        y1, m1, d1, y2, m2, d2 = y2, m2, d2, y1, m1, d1
    d_start = datetime.date(y1, m1, d1)
    d_end = datetime.date(y2, m2, d2)
    
    dfs = []
    df = []
    while d_start >= d_end:
        y, m, d = d_start.year, d_start.month, d_start.day
        print(y, m, d)
        if fu_op == "future":
            if info_type == "daily":
                df = [r for r in future_daily(y, m, d, "D", commodity)] \
                    + [r for r in future_daily(y, m, d, "N", commodity)]
            if info_type == "3m":
                df = [r for r in future_3m(y, m, d, commodity)]
        elif fu_op == "option":
            if info_type == "daily":
                df = [r for r in option_daily(y, m, d, "D", commodity)] \
                    + [r for r in option_daily(y, m, d, "N", commodity)]
            if info_type == "3m":
                df = [r for r in option_3m(y, m, d, commodity)]
        dfs = dfs + df
        d_start = d_start - datetime.timedelta(days = 1)
    return dfs

def sql_insert_n_values(table_name, n):
    pq_l = "(" + "?"
    pq_m = ", ?"
    pq_r = ")"
    pq = str()
    for i in range(n-1):
        pq = pq + pq_m
    pq = pq_l + pq + pq_r
    sql = "insert into " + table_name + " values " + pq
    return sql

def to_database(fu_op, commodity, info_type, y1, m1, d1, y2, m2, d2):
    fu_op = fu_op.lower()
    commodity = commodity.lower()
    info_type = info_type.lower()
    
    db_name = "tw_fx.db"
    db_address =  os.path.join(os.getcwd(), db_name)
    conn = sqlite3.connect(db_address)
    cur = conn.cursor()
    
    table_name = str()
    
    if fu_op == "future":
        table_name = table_name + "Future_"
        if commodity == "tx" or commodity == "txf":
            table_name = table_name + "TX_"
        elif commodity == "mtx" or commodity == "mxf":
            table_name = table_name + "MTX_"
        if info_type == "daily":
            table_name = table_name + "Daily"
            if commodity == "txf":
                commodity = "tx"
            if commodity == "mxf":
                commodity = "mtx"
            sql = sql_insert_n_values(table_name, 17)
        elif info_type == "3m":
            table_name = table_name + "3Major"
            if commodity == "tx":
                commodity = "txf"
            if commodity == "mtx":
                commodity = "mxf"
            sql = sql_insert_n_values(table_name, 13)
    elif fu_op == "option":
        table_name = table_name + "Option_"
        if commodity == "txo":
            table_name = table_name + "TXO_"
        if info_type == "daily":
            table_name = table_name + "Daily_Detail"
            sql = sql_insert_n_values(table_name, 19)
        elif info_type == "3m":
            table_name = table_name + "3Major"
            sql = sql_insert_n_values(table_name, 16)

    rows = load_data(fu_op, commodity, info_type, y1, m1, d1, y2, m2, d2)
    cur.executemany(sql, rows)
    
    conn.commit()
    conn.close()
    
#to_database("future", "tx", "daily", 2022, 3, 1, 2022, 3, 4)
to_database("option", "txo", "daily", 2022, 3, 1, 2022, 3, 4)
'''
for r in load_data("future", "TXF", "3m", 2022, 3, 3, 2022, 3, 5):
    print(r)
'''
'''
to_database("Future", "TX", "Daily", 2022, 3, 4, 2022, 3, 3)
to_database("future", "txf", "3M", 2022, 3, 4, 2022, 3, 3)
to_database("Future", "MTX", "Daily", 2022, 3, 4, 2022, 3, 3)
to_database("Future", "MTX", "3m", 2022, 3, 4, 2022, 3, 3)
to_database("Option", "TXO", "Daily", 2022, 3, 4, 2022, 3, 3)
to_database("Option", "TXO", "3m", 2022, 3, 4, 2022, 3, 3)
'''