#!/usr/bin/env python
#-*- coding:utf-8 -*-
#####################################################
#	File Name: fetch_stock_hist.py
#	Author: Rachel Liu
#	Created Time: 2017-05-15
#	Description: 
#	
#	Modified History: 
#	
#	Copyright (C)2017 All Rights Reserved
#####################################################
from selenium import webdriver
from bs4 import BeautifulSoup
import datetime
import pymysql
import time

class cat_stck_hist(object):
	global cursor, dbconn

	def __init__(self, cat):
		self.__url = 'http://www.aigaogao.com/tools/history.html?s=%s'
		self.__cat = cat
		self.__sql = "insert into MKT_STCK_HIST values(%s,%s,%s,%s,%s,%s,%s,%s)"
		cursor.execute("select distinct stock_id from Stocks_Details where sub_cat_cd =\'%s\' " %self.__cat)
		self.__catStock = [s[0] for s in cursor.fetchall()]
		self.loop_stock()

	## request webpage
	def fetch_stck_hist(self, stockID):
		stock = []
		# try:
			## open url like the chrome browser, in case that some infomation missing in dynamic loading
		browser = webdriver.Chrome()
		browser.get(self.__url%stockID)#
		# tell webdriver to poll the dom for a timeout，wait in a range	
		browser.implicitly_wait(30)
		## set retry time to wait for finding the element 
		for retry_n in range (3):
			try:
				# check if the specific element is responsed
				if browser.find_element_by_xpath("//div[@id ='ctl16_contentdiv']//tbody/tr"):
					break			
			except:
				# check if current stock contains the data
				if browser.find_element_by_xpath("//div[@id ='ctl16_contentdiv' and contains(text(), 'Please check symbol')]"):
					browser.close() 
					return 1	
			finally:
				print("%s page wait for finding element..."% (self.__url%stockID))
				browser.refresh() 
				time.sleep(5)

		# use beautifulsoup to decorate the page content
		soup = BeautifulSoup(browser.page_source, "html.parser")
		browser.close() 
		try: 
			contents = soup.find('div', attrs={"id":"ctl16_contentdiv"}).find('tbody').contents
		except AttributeError as e:
			print('Stock %s has error:\n %s' %(stockID, e))
			return 1

		# close the browser
		
		for info in contents[1: len(contents)-1]:
			hist = info.get_text(":").split(":")[0:7]
			hist.insert(0, stockID)
			stock.append(hist)
		# print(stock)
		
		cursor.executemany(self.__sql, stock) 
		dbconn.commit()


	## loop execution to fetch the stock history
	def loop_stock(self):
		stock_num = 0
		for stockID in self.__catStock:
			stock_num += 1

			if stock_num%600 == 0:
				time.sleep(60)

			if self.fetch_stck_hist(stockID):
				cursor.execute('insert into error_stock values(%s)', stockID)
				continue
					


if __name__=='__main__':

	start = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	try:
	    dbconn = pymysql.connect(host='localhost', user='root', passwd='root123', db='Feed_Crawler', charset='utf8')
	    print('DB connect sucessfully...')
	except pymysql.Error as e:
	    print("dbconn failed, please check if the server starts and the connect info !!!") 

	cursor = dbconn.cursor()

	c = cat_stck_hist('M003')
	
	cursor.close()
	dbconn.commit()

	end = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	print("################# stock fetch end at %s ...,start at %s !!######################\n" %(end,start))

