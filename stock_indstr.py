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
import datetime
import pymysql

NUM = {'M':0, 'I':0, 'G':0}
l = ['small', 'gem', 'ranklist_', 'industry_', 'blockperformance_3_']

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



def url_findresult(url, tag, attr):
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36"}
    request = urllib.request.Request(url=url,headers=headers)
    response = urllib.request.urlopen(request)
    result = response.read().decode('gbk')
    soup = BeautifulSoup(result, "html.parser")
    find_cont = soup.find(tag, attrs= attr)
    return find_cont

def stock_details(link, ltext, cat):
    lurl = 'http://quote.stockstar.com' + link   

    # category
    if cat == 'M': 
        suburl_p = lurl.replace('.shtml','') + '_1_0_%d.html'#order by stock_cd ascending 
    else:
        suburl_p = lurl.replace('.shtml','') + '_0_0_%d.html'
    pgnm = 1 ##page start at 1
    stocks = []  
    ## FETCH the data from each subclass          
    while True:
        suburl = suburl_p%pgnm
        content = url_findresult(suburl,'tbody', {"class": "tbody_right"}).contents
        rownum = len(content) - 1
        # check if current page is the last pagenum+1; in last pagenum+1, there is no stock in the list               
        if rownum == 0:
            break

        for stock in content[1:rownum]:
            stocks_l = stock.get_text(',').split(',')[0:2] ##stock_cd and stock_nm
            stocks_l += [lurl,ltext,cat]
            stocks.append(stocks_l) #create 2-d list, finnally insert into db
        print (stocks)
        pgnm += 1

    return 0


def fetch_stock_inds(url): ##stockstar web
    subNav = url_findresult(url, 'div', {"class": "subNav"})
    subNav_list = subNav.find_all('a')
    category = []
    for l in subNav_list:
        link = l['href']
        cat = cat_find(link)
        ltext = l.string.strip("· \t\n\r") #remove any space and . at the head/tail
        

        if cat:           
            # stock_details(link,ltext,cat)
            category.append([ltext,link,cat['cat'],cat['code']])
            print(category)

            # break

        
# # find the format rule of url
# # url = 'http://quote.stockstar.com/stock/ranklist_a_1_0_1.html'
# # [a] the stock type ;1-0-1 : [1]order by the first column(stock number); [0]: 0-->ascending 1-->desceding;[1]:page number


if __name__=='__main__':

    url = 'http://quote.stockstar.com/stock/industry.shtml'
    fetch_stock_inds(url)