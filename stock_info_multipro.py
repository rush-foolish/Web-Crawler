#!/usr/bin/env python3
#-*- coding:utf-8 -*-
#####################################################
#	File Name: stock_info_multipro.iy
#	Author: Rachel Liu
#	Created Time: 2017-05-03
#	Description: this is for the stock info fetched 
#	from stock star website, the frequence of the info
#	is 1/5 min/time in the period 9:00 - 15:00 
#	
#	Modified History: use the class to package the code
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

class stock_crawler(object):

    def __init__(self, db, type, pagenm):
        self.__db = db
        self.__table = 'stock_market_'+type
        self.__pagenm = pagenm
        self.__url = 'http://quote.stockstar.com/stock/ranklist_%s_1_0_'%type + '%d.html'
        self.__sql = "insert into {0} values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(self.__table)

    def dbconn(self):
        dbconcfg = {'host':'localhost',
                'user':'root',
                'passwd':'root123',
                'db': self.__db,
                'charset':'utf8'}
        try:
            conn = pymysql.connect(**dbconcfg)
            print('DB connect sucessfully...')
        except pymysql.Error as e:
            print("stock fetch failed as calling the function dbconn in %s, the error is %s" %(__name__, e))

        cursor = conn.cursor()
        
        # tb = {'conn':conn,'cursor':cursor,'sql':sql}
        self.__conn, self.__cursor = (conn, cursor)
        return {'conn': self.__conn, 'cursor': self.__cursor}

   
    def load_data(self, values):
        self.__cursor.executemany(self.__sql, values)

    @classmethod
    def fetch_stock(self, url): ##stockstar web
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
            p.apply_async(self.fetch_stock, args=(self.__url%i,), callback=self.load_data) 

        p.close()
        p.join()


# # find the format rule of url
# # url = 'http://quote.stockstar.com/stock/ranklist_a_1_0_1.html'
# # [a] the stock type ;1-0-1 : [1]order by the first column(stock number); [0]: 0-->ascending 1-->desceding;[1]:page number

if __name__=='__main__':

    start = datetime.datetime.now()

    print('Waiting for all subprocesses done...')

    # for stock in l :
        # print (stock['type'])
    s = stock_crawler('test', 'a', 107)

    db = s.dbconn()
    s.excute()

    db['cursor'].close()
    db['conn'].commit() 

    end = datetime.datetime.now()
    print('All subprocesses done. time usage %s' %(end-start))


