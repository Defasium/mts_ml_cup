import time as t
import os
import json
import re
import requests as r
import gzip
import pickle
from functools import reduce
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from bs4 import BeautifulSoup
import argparse

import functools

def main(args):
    offset = args.num_workers*args.i + args.k
    print(offset, args.num_workers, args.i, args.k)
    os.makedirs(args.savefolder, exist_ok=True)
    with open(args.url_file, 'r', encoding='utf-8') as f:
        domain = f.read().split('\n')[offset]
    domain = domain.strip()
    if (domain+'.gz') in os.listdir(args.savefolder): return

    if os.path.exists('tmp%d'%args.k):
        for _ in range(3):
            try:
                os.system("rm -r tmp%d"%args.k)
            except:
                t.sleep(1)
                continue

    caps = DesiredCapabilities().CHROME
    caps["pageLoadStrategy"] = "none"   # Do not wait for full page load
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--user-data-dir=tmp%d"%args.k)
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disk-cache-size=0")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})

    driver = webdriver.Chrome(executable_path='chromedriver.exe',
                              chrome_options=chrome_options,
                              desired_capabilities=caps)
    driver.delete_all_cookies()

    driver.get('https://www.similarweb.com/website/%s'%('.'.join(domain.split('.')[-2:])))

    for _ in range(30):
        t.sleep(1)
        try:
            driver.execute_script("window.scrollBy(0,document.getElementById('geography').scrollHeight)")
        except: pass
        content = driver.page_source
        if 'wa-demographics__age-data-label' in content: break
    for _ in range(5):
        t.sleep(1)
        if 'wa-demographics__age-data-label' in content:
            print(offset, args.num_workers, args.i, args.k, domain, 'ok')
            with open('simweblog_%d.log'%args.k, 'a') as f:
                f.write('%s\t%s\n'%(domain, BeautifulSoup(content, 'html.parser').find({'class':'wa-demographics'}).text))
            break
    with gzip.open(os.path.join(args.savefolder, domain+'.gz'), 'wb') as f:
        f.write(content.encode('utf-8'))
    driver.close()
    driver.quit()
    try:
        os.system("rm -r tmp%d"%args.k)
    finally:
        return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-k", type=int, help="worker id")
    parser.add_argument("-i", type=int, help="step id", default=0)
    parser.add_argument("--num_workers", type=int, help="number of workers", default=10)
    parser.add_argument("--savefolder", type=str, help="folder with html content", default='simwebhtml')
    parser.add_argument("--url_file", type=str, help="file with urls per line", default='urls160323.txt')
    args = parser.parse_args()
    try:
        main(args)
    except KeyboardInterrupt:
        pass
