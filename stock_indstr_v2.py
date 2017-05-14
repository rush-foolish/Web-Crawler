#!/usr/bin/env python3
#-*- coding:utf-8 -*-
#####################################################
#	File Name: stock_indstr_v2.py
#	Author: Rachel Liu
#	Created Time: 2017-05-13
#	Description: As the stock_industr need more than 30 min
#   to run completely, do some improvement
#	
#	Modified History: 
#	
#	Copyright (C)2017 All Rights Reserved
#####################################################

import urllib.request
from bs4 import BeautifulSoup
import datetime
import pymysql
import time
import http

def cat_find(s):       
    global NUM,l
    if s.find(l[0])>0 or s.find(l[1])>0 or s.find(l[2])>0:
        cat = 'M'
    elif s.find(l[3])>0:
        cat = 'I'
    elif s.find(l[4])>0:
        cat = 'G'
    else:
        return False
    NUM[cat] += 1

    return { 'cat': cat, 'code': cat+'%03d'%NUM[cat] }

def load_db(table, values):
    global cursor
    sql = "insert into {0} values(%s,%s,%s)".format(table)
    if table == 'Sub_Category':
        sql = sql = "insert into {0} values(%s,%s,%s,%s)".format(table)
    cursor.executemany(sql, values)

## request webpage
def url_request(url):
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36"}
    request = urllib.request.Request(url=url,headers=headers)
    try:      
        response = urllib.request.urlopen(request)
        result = response.read().decode('gbk')
    except:
        print(url)
        time.sleep(10)
        connection = http.client.HTTPConnection(url.lstrip("http://"))
        connection.request('GET')
        result = connection.getresponse().read().decode('gbk')

    soup = BeautifulSoup(result, "html.parser")
    return soup

## fetch stock details
def stock_details(lurl, pagenum, cat):  
    # prefix of sub－category url
    if cat.startswith('M'): 
        suburl_p = lurl.replace('.shtml','') + '_1_0_%d.html'#order by stock_cd ascending 
    else:
        suburl_p = lurl.replace('.shtml','') + '_0_0_%d.html'

    stocks = []  
    ## FETCH the data from each subclass, loop each page          
    for i in range(1, pagenum+1):
        soup = url_request(suburl_p%i)
        content = soup.find('tbody', attrs={"class": "tbody_right"}).contents
        for stock in content[1:len(content)-1]: #  the first list element is "\n", start from the second one
            stocks_l = stock.get_text(',').split(',')[0:2] ##stock_cd and stock_nm
            stocks_l.append(cat)
            stocks.append(stocks_l) #create 2-d list, finnally insert into db
    # print(stocks)
    load_db("Stocks_Details", stocks)

## fetch subcategory
def fetch_stock_inds(url):  
    subNav = url_request(url).find('div', attrs={"class": "subNav"})
    subNav_list = subNav.find_all('a')
    category = []
    for l in subNav_list:
        lurl = 'http://quote.stockstar.com' + l['href']
        # find current sub category information
        cat = cat_find(lurl)

        # if cat is in the list, find the stock info 
        if cat:
            ltext = l.string.strip("· \t\n\r") #category name:remove any space and . at the head/tail
            category.append([cat['code'],ltext,lurl,cat['cat']])
            #find page number info 
            page = url_request(lurl).find('input', attrs={"id": "ClientPageControl1_hdnTotalCount"})

            # check if current sub category has the stock data: if yes, find the stock details
            if page.has_attr('value'):
                pagenum = int(page['value'])//30 + (29+int(page['value'])%30)//30
                # fetch stock details
                stock_details(lurl,pagenum,cat['code'])
    # print(category)
    load_db('Sub_Category', category)


        
# # find the format rule of url
# # url = 'http://quote.stockstar.com/stock/ranklist_a_1_0_1.html'
# # [a] the stock type ;1-0-1 : [1]order by the first column(stock number); [0]: 0-->ascending 1-->desceding;[1]:page number

if __name__=='__main__':

    ## initialize the global sub category number, to obtain the sub_cat_code
    NUM = {'M':0, 'I':0, 'G':0}
    l = ['/small', '/gem', 'ranklist_', 'industry_', 'blockperformance_3_']

    ## connect db, all use the same db
    try:
        dbconn = pymysql.connect(host='localhost', user='root', passwd='root123', db='Feed_Crawler', charset='utf8')
        print('DB connect sucessfully...')
    except pymysql.Error as e:
        print("dbconn failed, please check if the server starts and the connect info !!!") 
    
    cursor = dbconn.cursor() 

    ## fetch main page
    url = 'http://quote.stockstar.com/stock/industry.shtml'
    fetch_stock_inds(url)

    cursor.close()
    dbconn.commit() 
