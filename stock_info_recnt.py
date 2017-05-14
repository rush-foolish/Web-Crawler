#!/usr/bin/env python3
#-*- coding:utf-8 -*-
#####################################################
#	File Name: stock_info_recnt.py
#	Author: Rachel Liu
#	Created Time: 2017-05-02
#	Description: this is for the stock info fetched 
#	from stock star website, the frequence of the info
#	is 1/5 min/time in the period 9:00 - 15:00 
#	
#	Modified History: 
#	
#	Copyright (C)2017 All Rights Reserved
#####################################################

import urllib.request
from bs4 import BeautifulSoup
from multiprocessing import Pool
import datetime,time 
import pymysql

# multiprocessing may cause zombie process
# bug: https://mail.python.org/pipermail/python-bugs-list/2015-December/287698.html
# http://stackoverflow.com/questions/30506489/python-multiprocessing-leading-to-many-zombie-processes


# def writerow_csv(row_line):
#     file_name = '\\Users\\liuruiqin1\\Desktop\\test'
#     with open(file_name + '.csv', 'a', newline = '',encoding='utf-8') as file:
#         file.write(row_line)

class stock_crawler(object): ##stockstar web
    
    # @classmethod
    def __init__(self, stock):
        self.__cursor = stock['cursor']
        self.__table = 'stock_market_' + stock['type']
        self.__pagenm = stock['pgnm']
        self.__url = 'http://quote.stockstar.com/stock/ranklist_%s_1_0_' %stock['type'] + '%d.html'
        
        if stock['type'] in ('a' , 'b'):
            self.__sql = "insert into {0} values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(self.__table)
        else:
            self.__sql = "insert into {0} values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(self.__table)
        #when implementing constructor, intialize the variate/parameter and excute the stock fetch
        print("stock %s is begining at %s"%(self.__table,datetime.datetime.now().strftime('%H:%M:%S')))
        self.excute()
   
    def load_data(self, values):
        try:
            self.__cursor.executemany(self.__sql, values)
        except pymysql.Error as e:
            print('Insert table %s error, check the insert script: \n %s' %(self.__table, self.__sql))

    @classmethod
    def fetch_stock(self, url): 
        headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36"}
        request = urllib.request.Request(url=url,headers=headers)
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
        for stockinfo in content[1:len(content)-1]: ##the first row is as same as the last row of previous page
            # for string in stockinfo.strings: #find the text in the table rowlist
            #     print(string)
            info = stockinfo.get_text(',').split(',') # change the str to list
            info.append(dt)
            rowline.append(info)
        return rowline

    def excute(self):
        p = Pool(4)
        for i in range(1,self.__pagenm+1):
            #callback will get stuck, but can call another function by using the return-result
            p.apply_async(self.fetch_stock, args=(self.__url%i,), callback=self.load_data) 
	
        p.close()
        p.join()


def dbconn(db):
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

    return {'conn': conn, 'cursor': cursor}


# # find the format rule of url
# # url = 'http://quote.stockstar.com/stock/ranklist_a_1_0_1.html'
# # [a] the stock type ;1-0-1 : [1]order by the first column(stock number); [0]: 0-->ascending 1-->desceding;[1]:page number

if __name__=='__main__':

    start = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    db = dbconn('test')
    lstock = [ {'type':'small', 'pgnm':29, 'cursor':db['cursor']}, 
        {'type':'gem', 'pgnm':21, 'cursor':db['cursor']},
        {'type':'a', 'pgnm':107, 'cursor':db['cursor']},
        {'type':'b', 'pgnm':4, 'cursor':db['cursor']} ]

    print('Waiting for all subprocesses done...')

    p = Pool(4)

    for i in range(len(lstock)) :
        # print(lstock[i])
        # stock_crawler(lstock[i])
        p.apply_async( stock_crawler(lstock[i]) ) 
		
         # t = stock_crawler({'cursor':db['cursor'], 'type':s['type'], 'pagenm':s['pgnm']})    
    p.close()
    p.join()
    db['cursor'].close()
    db['conn'].commit() 

    end = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("################# stock fetch end at %s ...,start at %s !!######################\n" %(end,start) )



