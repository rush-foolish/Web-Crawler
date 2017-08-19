#!/usr/bin/env python3
#-*- coding:utf-8 -*-
#####################################################
#	File Name: stock_indstr.py
#	Author: Rachel Liu
#	Created Time: 2017-05-09
#	Description: 
#	
#	Modified History: 
#	
#	Copyright (C)2017 All Rights Reserved
#####################################################

import urllib.request
from bs4 import BeautifulSoup
# from multiprocessing import Pool
import time
import pymysql
import re
# import http.client

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
        sql = "insert into {0} values(%s,%s,%s,%s)".format(table)
    cursor.executemany(sql, values)

## request webpage
def url_findresult(url, tag, attr={}):
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36"}
    request = urllib.request.Request(url=url,headers=headers)
    try:      
        response = urllib.request.urlopen(request)
        result = response.read().decode('gbk')
    except:
        time.sleep(10)
        response = urllib.request.urlopen(request)
        result = response.read().decode('gbk')

    soup = BeautifulSoup(result, "html.parser")
    find_cont = soup.find(tag, attrs= attr)
    return find_cont

## fetch stock details
def stock_details(link, cat):  
    lurl = 'http://quote.stockstar.com' + link   
    # prefix of sub－category url
    if cat.startswith('M'): 
        suburl_p = lurl.replace('.shtml','') + '_1_0_%d.html'#order by stock_cd ascending 
    else:
        suburl_p = lurl.replace('.shtml','') + '_0_0_%d.html'
    pgnm = 1 ##page start at 1
    stocks = []  
    ## FETCH the data from each subclass          
    while True:
        suburl = suburl_p%pgnm
        try:
            content = url_findresult(suburl,'tbody', {"class": "tbody_right"}).contents
        except AttributeError as e:
            print(suburl)
            print(e)

        rownum = len(content) - 1
        # check if current page is the last pagenum+1; in last pagenum+1, there is no stock in the list               
        if rownum < 1:
            break
        # check if the page has the stock data    
        if content[1].get_text().find('暂无数据') > -1:
            stocks.append(['暂无数据', '暂无数据', cat])
            break

        for stock in content[1:rownum]: #  the first list element is "\n", start from the second one
            stocks_l = stock.get_text(',').split(',')[0:2] ##stock_cd and stock_nm
            stocks_l.append(cat)
            stocks.append(stocks_l) #create 2-d list, finnally insert into db
        pgnm += 1
    # print(stocks)
    load_db("Stocks_Details", stocks)

## fetch subcategory
def fetch_stock_inds(url): 
    subNav = url_findresult(url, 'div', {"class": "subNav"})
    subNav_list = subNav.find_all('a')
    category = []
    for l in subNav_list:
        link = l['href']
        cat = cat_find(link)
        ltext = l.string.strip("· \t\n\r") #remove any space and . at the head/tail
		# if cat is in the list, find the stock info 
        if cat:        
            category.append([cat['code'],ltext,link,cat['cat']])
            stock_details(link,cat['code'])
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
    dbconn = pymysql.connect(host='localhost', user='root', passwd='root123', db='Feed_Crawler', charset='utf8')
    cursor = dbconn.cursor() 

    ## fetch main page
    url = 'http://quote.stockstar.com/stock/industry.shtml'
    fetch_stock_inds(url)
    # stock_details("/stock/industry_o.shtml","I")
