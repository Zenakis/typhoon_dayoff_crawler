#import typhoon-stop-work-spider
import schedule
import time
import os


def job():
  os.system("scrapy runspider /Users/mycenamaster/Documents/Development/code/python/typhonStopWorkSpider/typhoon-stop-work-spider.py")

schedule.every(1).minutes.do(job)

while 1:
  schedule.run_pending()
  time.sleep(1)
