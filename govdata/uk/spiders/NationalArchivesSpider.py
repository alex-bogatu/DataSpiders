'''
Created on 29 Jan 2018

@author: alex
'''

import re
import os
import scrapy
import urllib
import string
import random
import json
from scrapy.crawler import CrawlerProcess

ORIGIN_URL = "http://webarchive.nationalarchives.gov.uk/search/result/?q=NHS%20hospitals&mime=CSV"
# ORIGIN_URL = "http://webarchive.nationalarchives.gov.uk/search/result/?q=NHS%20GP%20%20%20&include=&exclude=&site=&site_exclude=reference.data.gov.uk&mime=CSV&amount=100"
# ORIGIN_URL = "http://webarchive.nationalarchives.gov.uk/search/result/?q=real%20estate%20%20%20%20&include=&exclude=&site=homesandcommunities.co.uk&site_exclude=reference.data.gov.uk&mime=CSV&include=&amount=100"
SAVETO = "/home/alexU/datasets/natarch/nhs/"

class NatArchSpider (scrapy.Spider):
    
    name = 'data.gov.uk'
    download_delay = 0.2
#     download_warnsize = 134217728 #128Mb
    
    def start_requests(self):
        
        page_req = scrapy.Request(url=ORIGIN_URL, callback=self.parse)
        yield page_req
    
    def parse(self, response):
        if (response.status != 200):
            print 'Request to ' + response.url + 'returned ' + str(response.status)
            return
        
        for p in range(1, 401):
            page_url = ORIGIN_URL + '&page=' + str(p)
            page_req = scrapy.Request(url=page_url, callback=self.parsePage)
            yield page_req
            
    def parsePage(self, response):
        if (response.status != 200):
            print 'Request to ' + response.url + 'returned ' + str(response.status)
            return
        
        
        for path in response.xpath('//*[@id="result-list"]//div[@class="header"]/h4[@class="title"]/a/@href'):
#             print path.extract()
            category_url = response.urljoin(path.extract())
            category_req = scrapy.Request(url=category_url, callback=self.saveDataset)
            yield category_req
    
    def saveDataset(self, response):
        if (response.status != 200):
            print 'Request for dataset + ' + response.meta.get('OriginalName') + "(" + response.url + ') returned ' + str(response.status)
            return
        dsName = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(10))
        print "Saving dataset " + dsName + " to " + SAVETO + " ..."
        
        file_path = SAVETO + dsName + '.csv' 
        with open(file_path, 'wb') as f:
            f.write(response.body)
        print 'Saved'
        
        
if __name__ == '__main__':
    crawler = CrawlerProcess({'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'})
    crawler.crawl(NatArchSpider)
    crawler.start()
    