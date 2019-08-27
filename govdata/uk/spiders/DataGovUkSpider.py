'''
Created on 29 Jan 2018

@author: alex
'''

import re
import os
import scrapy
import urllib
import logging
import string
import random
import json
from scrapy.crawler import CrawlerProcess

ORIGIN_URL = "https://data.gov.uk"
SAVETO = "/home/alexU/datasets/"
REPO_BASE_DIR = "/home/alexU/datasets/"

DATAGOV_DOMAINS = [
    'Business & Economy',
    'Crime & Justice',
    'Defence',
    'Education',
    'Environment',
    'Government',
    'Government Spending',
    'Health',
    'Mapping',
    'Society',
    'Towns & Cities',
    'Transport'
    ]

FORMAT = 'CSV';
RANDOMNAME_SIZE = 7

class DataGovUkSpider (scrapy.Spider):
    
    name = 'data.gov.uk'
    download_delay = 0.3
    download_warnsize = 134217728 #128Mb
    
    logging.basicConfig(filename=REPO_BASE_DIR + name + ".log", level=logging.DEBUG)
    
    def start_requests(self):
        request_params = {'broken_links': 'OK',
                          'res_format': FORMAT,
                          'theme-primary': ''}
        for domain in DATAGOV_DOMAINS:
            request_params['theme-primary'] = domain
            url_params = urllib.urlencode(request_params)
            page_url = ORIGIN_URL + '/data/search?' + url_params
            page_req = scrapy.Request(url=page_url, callback=self.parse, meta={'Domain': domain})
            yield page_req
    
    def parse(self, response):
        if (response.status != 200):
            logging.warning('Request to ' + response.url + 'returned ' + str(response.status))
            return
        
        last_page = response.xpath('//div[@class="dgu-pagination"]/ul[@class="pagination"]/li[position()=(last()-1)]/a/text()').extract()[0]
        for i in range(1, int(last_page)+1):
            page_url = response.url + "&page=" + str(i)
            page_req = scrapy.Request(url=page_url, callback=self.parsePage, meta={'Domain': response.meta.get('Domain')})
            yield page_req
            
    def parsePage(self, response):
        if (response.status != 200):
            logging.warning('Request to ' + response.url + 'returned ' + str(response.status))
            return
        for path in response.xpath('//a[@class="dataset-header"]/@href'):
            category_url = response.urljoin(path.extract())
            category_req = scrapy.Request(url=category_url, callback=self.downloadDatasets, meta={'Domain': response.meta.get('Domain')})
            yield category_req
    
    def downloadDatasets(self, response):
        resourceMetadata = {}
        if (response.status != 200):
            logging.warning('Request to ' + response.url + 'returned ' + str(response.status))
            return
        
        resourceMetadata['Domain'] = response.meta.get('Domain')
        
        try:
            category = response.xpath('//div[@class="module-content"]/div[@class="package"]/h1/text()').extract()[0].lstrip().rstrip()
            resourceMetadata['Category'] = re.sub(r'[\n\t\s]+', ' ', category)
        except:
            logging.warning('No category information found for ' + response.url) 
                
        try:
            publisher = response.xpath('//div[@id="license-info"]/a/text()').extract()[0].rstrip().lstrip()
            publisherUrl = response.xpath('//div[@id="license-info"]/a/@href').extract()[0].rstrip().lstrip()
            resourceMetadata['Publisher'] = re.sub(r'[\n\t\s]+', ' ', publisher)
            resourceMetadata['Publisher_URL'] = response.urljoin(re.sub(r'[\n\t\s]+', ' ', publisherUrl))
        except:
            logging.warning('No publisher information found for ' + response.url)
        
        resource_pool = response.xpath('//div[@class="dataset-resources"]//div[@class="dataset-resource"]')
        resource_counter = 0
        for resource in resource_pool:
            resource_counter = resource_counter + 1
            try:
                dataset_format = resource.xpath('./div[@class="dataset-resource-format"]/span[@class="format-name"]/text()').extract()[0].lstrip().rstrip()
                dataset_format = re.sub(r'[\n\t\s]+', ' ', dataset_format)
            except:
                logging.warning('No format information found for resource ' + str(resource_counter) + ", skipping ...")
                continue
            
            if dataset_format != FORMAT:
                continue
            
            try:
                dataset_location = resource.xpath('./div[@class="dataset-resource-text"]/div[@class="inner"]/div[@class="inner-row actions"]/div[@class="inner-cell"]/span[last()]/a/@href').extract()[0].rstrip().lstrip()
                dataset_original_name = dataset_location.split("/")[-1]
            except:
                logging.warning('No location information found for resource ' + str(resource_counter) + ", skipping ...")
                continue
            
            datasetMetadata = resourceMetadata.copy()
            datasetMetadata['Format'] = dataset_format
            datasetMetadata['Location'] = dataset_location
            datasetMetadata['OriginalName'] = dataset_original_name
            
            try:
                dataset_description = resource.xpath('./div[@class="dataset-resource-text"]/div[@class="inner"]/div[@class="inner-row description"]/span[@class="inner-cell"]/text()').extract()[0].lstrip().rstrip()
                datasetMetadata['Description'] = re.sub(r'\r?[\n\t\s]+', ' ', dataset_description)
            except:
                logging.warning('No description information found for resource ' + str(resource_counter))
            
            random_name = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(RANDOMNAME_SIZE))
            datasetMetadata['LocalName'] = random_name + "_" + re.sub(r'\%[0-9]+', "_", dataset_original_name)
            
            dataset_req = scrapy.Request(url=dataset_location, callback=self.saveDataset, meta={'metadata': datasetMetadata})
            yield dataset_req
            
    def saveDataset(self, response):
        if (response.status != 200):
            logging.warning('Request for dataset + ' + response.meta.get('OriginalName') + "(" + response.url + ') returned ' + str(response.status))
            return
        
        datasetMetadata = response.meta.get('metadata')
        dir_path = REPO_BASE_DIR + self.name + "/" + re.sub(r'[\s\&]+', '', datasetMetadata.get('Domain'))
        
        logging.info("Saving dataset " + datasetMetadata.get('OriginalName') + " to " + dir_path + " ...")
        
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        
        file_path = dir_path + "/" + datasetMetadata.get('LocalName')
        with open(file_path, 'wb') as f:
            f.write(response.body)
            
        logging.info("Dataset " + datasetMetadata.get('OriginalName') + " successfully saved : " + file_path + ".")
        
        datasetMetadata['LocalPath'] = file_path
        
        logging.info("Saving metadata for dataset " + datasetMetadata.get('OriginalName') + " ...")
        metadata_path = REPO_BASE_DIR + self.name + "/" + self.name + ".metadata.json"
        with open(metadata_path, 'ab') as mf:
            json.dump(datasetMetadata, mf)
            mf.write("\n")
        logging.info("Metadats for dataset " + datasetMetadata.get('OriginalName') + " successfully saved.")
        
        
if __name__ == '__main__':
    crawler = CrawlerProcess({'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'})
    crawler.crawl(DataGovUkSpider)
    crawler.start()
    