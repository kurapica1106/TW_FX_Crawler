import csv
import sqlite3

db_address = r"D:\tw_fx_crawler\tw_fx.db"
con = sqlite3.connect(db_address)
cur = con.cursor()

sql = " \
    select * from Option_TXO_3Major \
    "
cur.execute(sql)
results_op3m = cur.fetchall()

for r in results_op3m:
    sql = " \
        select sum(V), sum(OI) from Option_TXO_Daily_Detail \
        where Year = %d and Month = %d and Day = %d and CP = '%s' \
        " % (r[2], r[3], r[4], r[6])
    cur.execute(sql)
    results_opdly = cur.fetchall()
    ttl_v = results_opdly[0][0] if len(results_opdly) > 0 else int()
    ttl_oi = results_opdly[0][1] if len(results_opdly) > 0 else int()
    ppt_v = r[8] / ttl_v
    ppt_oi = r[11] / ttl_oi
    sql = " \
        update Option_TXO_3Major \
        set 交易量占比 = %f, 未平倉量占比 = %f \
        where Year = %d and Month = %d and Day = %d \
            and 身分 = '%s' and CP = '%s' and BS = '%s' \
        " % (ppt_v, ppt_oi, r[2], r[3], r[4], r[5], r[6], r[7])
    cur.execute(sql)

'''
sql = " \
    select sum(V), sum(OI) from Option_TXO_Daily_Detail \
    where Year = %d and Month = %d and Day = %d and CP = '%s' \
    " % (results_op3m[0][2], results_op3m[0][3], results_op3m[0][4], results_op3m[0][6])
cur.execute(sql)
results_opdly = cur.fetchall()
ttl_v = results_opdly[0][0] if len(results_opdly) > 0 else int()
ttl_oi = results_opdly[0][1] if len(results_opdly) > 0 else int()
ppt_v = results_op3m[0][8] / ttl_v
ppt_oi = results_op3m[0][11] / ttl_oi

sql = " \
    update Option_TXO_3Major \
    set 交易量占比 = %f, 未平倉量占比 = %f \
    where Year = %d and Month = %d and Day = %d \
        and 身分 = '%s' and CP = '%s' and BS = '%s' \
    " % (ppt_v, ppt_oi,
         results_op3m[0][2], results_op3m[0][3], results_op3m[0][4],
         results_op3m[0][5], results_op3m[0][6], results_op3m[0][7])
cur.execute(sql)
'''
con.commit()
con.close()
