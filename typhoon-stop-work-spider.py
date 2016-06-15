# -*- coding: utf-8 -*-
import scrapy
import json
from scrapy.selector import Selector
import os.path
import codecs
import time
from kafka import KafkaProducer
from kafka.common import KafkaError

class TyphoonJson(object):
    def __init__(self, updated, announcement_list):
        self.updated = updated
        self.announcement_list = announcement_list
        
class TyphoonMessage(object):
    def __init__(self, county, announced):
        self.county = county
        self.announced = announced
    
class TyphoonStopWorkSpider(scrapy.Spider):
  
    name = 'typhoonstopworkspider'
    start_urls = ['http://www.dgpa.gov.tw/nds.html']    
    KAFKA_SERVER_IP = '59.127.187.54'
    KAFKA_SERVER_PORT = '9092'
    KAFKA_TOPIC = 'typhon_dayoff_announcement'
    producer = KafkaProducer(bootstrap_servers=['59.127.187.54:9092'])

    #爬出人事行政總處網頁資訊
    def parse(self,response):
        #table > tbody > tr > td > p > font::text
        for body in response.css('body').extract():
            body_content = body
        self.check_update_time(body_content)

    #確認資訊更新時間
    def check_update_time(self,body_content):
        if os.path.exists('./update_time.txt'):
            text_file = codecs.open('update_time.txt', 'r', 'utf-8')
            parse_update_time = Selector(text=body_content).xpath('//table/tbody/tr/td/p/font/text()')
            if parse_update_time[2].extract() == unicode(text_file.readlines()[0].encode('utf-8'),'utf-8'):
                print "There was nothing update..."
            else:
                print parse_update_time.extract()[2].encode('utf-8')
                self.save_update_time(parse_update_time.extract()[2].encode('utf-8'))
                self.parse_content(body_content,parse_update_time.extract()[2].encode('utf-8'));
            text_file.close()
        else:
            parse_update_time = Selector(text=body_content).xpath('//table/tbody/tr/td/p/font/text()')
            self.save_update_time(parse_update_time.extract()[2].encode('utf-8'))
            self.parse_content(body_content, parse_update_time.extract()[2].encode('utf-8'));

    #儲存更新時間        
    def save_update_time(self,update_time):
        text_file = open("update_time.txt","w")
        text_file.write(update_time)
        text_file.close()
        print 'Update time saved.'

    #解析內文資訊    
    def parse_content(self,body_content,update_time):
        #解析表格Title
        parse_body_content = Selector(text=body_content).xpath('//table/tbody/tr/td/font/h2/text()')
        if not parse_body_content:
            parse_detail = ''
            for parse_body_content in Selector(text=body_content).xpath('//table/tbody/tr/td/font/text()'):
                parse_detail += parse_body_content.extract()+","
            self.parse_to_json(parse_detail,update_time)
        else:
    #            print "parse data : " + parse_body_content[0].extract()
            for parse_body_content in Selector(text=body_content).xpath('//table/tbody/tr/td/font/h2/text()'):
                if parse_body_content.extract() == unicode('無停班停課訊息。','utf-8'):
                    print "目前無停班停課訊息。"
                else:
                    print "Something Happened..."

    def jdefault(self,o):
        if isinstance(o,set):
            return list(o)
        return o.__dict__

    def parse_to_json(self,parse_detail,update_time):
        parse_detail_list = parse_detail.split(',')

        dayoff_list = []

        for index in range(len(parse_detail_list)-1):
            if index == 0:
                print parse_detail_list[index]
            elif index%2 == 1:
                location = parse_detail_list[index]
            elif index%2 == 0:
                dayoff_list.append(TyphoonMessage(location, parse_detail_list[index].replace(' ','')))

        #日期處理
        date = update_time.split('：')[1].replace(" ","");    

        typhoonJsonObj = TyphoonJson(time.mktime(time.strptime(date , "%Y/%m/%d%H:%M:%S")),dayoff_list)

#        typhoonJson = json.dumps(typhoonJsonObj, default=self.jdefault , ensure_ascii=False)
        self.sendMsgToKafka(typhoonJsonObj)
        
    def sendMsgToKafka(self, typhoonJsonObj):
#        print typhoonJson.encode('utf-8')
        producer = KafkaProducer(bootstrap_servers=[self.KAFKA_SERVER_IP + ":" + self.KAFKA_SERVER_PORT], value_serializer=lambda v: json.dumps(v,default=self.jdefault , ensure_ascii=False).encode('utf-8'))
#        producer.send('test', {'foo':'bar'})
#        producer = KafkaProducer(bootstrap_servers=['59.127.187.54:9092'])
        future = producer.send('typhon_dayoff_announcement', typhoonJsonObj)
        # Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            # Decide what to do if produce request failed...
            log.exception()
            pass
#        producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
#        self.producer.send('typhon_dayoff_announcement', typhoonJson)
