#!/usr/bin/env python3
#-*- coding:utf-8 -*-
#####################################################
#	File Name: stock_info_loaddb.py
#	Author: Rachel Liu
#	Created Time: 2017-05-02
#	Description: this is for the stock info fetched 
#	from stock star website, the frequence of the info
#	is 5 min/time in the period 9:00 - 15:00 
#	
#	Modified History: 
#	
#	Copyright (C)2017 All Rights Reserved
#####################################################

import urllib.request
from bs4 import BeautifulSoup
from multiprocessing import Pool
import datetime
import pymysql




# def writerow_csv(row_line):
#     file_name = '\\Users\\liuruiqin1\\Desktop\\test'
#     with open(file_name + '.csv', 'a', newline = '',encoding='utf-8') as file:
#         file.write(row_line)

def dbconn(db, table):
    dbconcfg = {'host':'localhost',
            'user':'root',
            'passwd':'root123',
            'db': db,
            'charset':'utf8'}
    try:
        conn = pymysql.connect(**dbconcfg)
        print('DB connect sucessfully...')
    except pymysql.Error as e:
        print("stock fetch failed as calling the function dbconn in %s, the error is %s" %(__name__, e))

    cursor = conn.cursor()
    sql = "insert into {0} values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(table)

    return {'conn':conn,'cursor':cursor,'sql':sql}


def load_data(values):
    tb['cursor'].executemany(tb['sql'], values)


def fetch_stock(url): ##stockstar web
    headers={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36"}
    request=urllib.request.Request(url=url,headers=headers)
    response = urllib.request.urlopen(request)
    result = response.read().decode('gbk')
    soup = BeautifulSoup(result, "html.parser")
    # time.sleep(0.01)
    ## obtain data time
    datatime = soup.find('span', attrs={"id": "datatime"})
    dt = datatime.get_text()[5:]
    ## obtain tbody
    tbody = soup.find('tbody')  ##find the stockinfo in one page
    content = tbody.contents  ##obtain the stock list
    rowline = []
    for stockinfo in content[1:len(content)-1]:
        # for string in stockinfo.strings: #find the text in the table rowlist
        #     print(string)
        info = stockinfo.get_text(',').split(',') # change the str to list
        info.append(dt)
        rowline.append(info)
    return(rowline)

# # find the format rule of url
# # url = 'http://quote.stockstar.com/stock/ranklist_a_1_0_1.html'
# # [a] the stock type ;1-0-1 : [1]order by the first column(stock number); [0]: 0-->ascending 1-->desceding;[1]:page number

if __name__=='__main__':

    start = datetime.datetime.now()
    
    tb = dbconn('test','test3')
  
    url = 'http://quote.stockstar.com/stock/ranklist_a_1_0_%d.html'
    p = Pool(50)
    for i in range(1,107):
        p.apply_async(fetch_stock, args=(url%i,), callback=load_data) #in case callback=writerow_csv)
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()

    tb['cursor'].close()
    tb['conn'].commit() 
    
    end = datetime.datetime.now()
    print('All subprocesses done. time usage %s' %(end-start))


