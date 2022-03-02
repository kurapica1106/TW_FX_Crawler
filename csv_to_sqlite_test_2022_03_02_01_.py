import csv
import datetime
import sqlite3

csv_address = r"D:\tw_fx_crawler\抓好的資料\op_day_data.csv"
csv_file = open(csv_address)
rows = csv.reader(csv_file)
rows = list(rows)
rows.pop(0)

new_rows = []
for row in rows:
    Y = int(row[0])
    M = int(row[1])
    D = int(row[2])
    YMD = Y * 10000 + M * 100 + D
    W = datetime.datetime(Y, M, D).weekday() + 1
    
    DN = "D"
    
    if row[5].lower() == "call":
        CP = "C"
    else:
        CP = "P"
    
    if len(row[3]) == 8:
        C_Y = int(row[3][0:4])
        C_M = int(row[3][4:6])
        C_W = int(row[3][7])
    else:
        C_Y = int(row[3][0:4])
        C_M = int(row[3][4:6])
        C_W = 3
    C_YMW = C_Y * 1000 + C_M * 10 + C_W
    
    StrP = int(row[4])
    
    ID = "%d_%s_%s_%d_%05d" % (YMD, DN, CP, C_YMW, StrP)
    
    O = float(row[6]) if row[6] != "-" else float()
    H = float(row[7]) if row[7] != "-" else float()
    L = float(row[8]) if row[8] != "-" else float()
    C = float(row[9]) if row[9] != "-" else float()
    
    SetP = int()
    
    V = float(row[10]) if row[10] != "-" else float()
    OI = float(row[11]) if row[11] != "-" else float()
    
    new_row = [ID, YMD, Y, M, D, W, DN, CP, C_YMW, C_Y, C_M, C_W, StrP, O, H, L, C, SetP, V, OI]
    new_rows.append(new_row)

db_address = r"D:\tw_fx_crawler\tw_fx.db"
con = sqlite3.connect(db_address)
cur = con.cursor()
table_name = "Option_TXO_Daily_Detail"
sql = "insert into " + table_name + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
'''
sql = "delete from " + table_name
cur.execute(sql)
'''
cur.executemany(sql, new_rows)
con.commit()
con.close()