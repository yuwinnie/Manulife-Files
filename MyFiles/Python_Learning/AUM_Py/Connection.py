from pyhive import hive
import  re, os, time

host_name = "azcedlmstv001.v01caedl.manulife.com"
port = 8020
user = "yuwinni@MFCGD.COM"
password = "Monday*1"
database = "inv_aum_raw_qa"

def hiveconnection(host_name,port,user,pasword,database):

    conn = hive.connection(
            host = host_name,
            port = port,
            username = user,
            password = password,
            database = database)
    cur = conn.cursor()
    cur.execute('select count(*) from inv_aum_raw_qa.test_dpl_gngnco;')
    result = cur.fetchall()
    
    return result
    
output = hiveconnection(host_name,port,user,password,database)
print(output)
