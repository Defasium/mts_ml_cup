import pyautogui as p
import time as t
import os
import numpy as np
import pandas as pd
import json
import re
import requests as r
from PIL import Image
import gzip
import pickle
from functools import reduce
import pyperclip
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from subprocess import Popen, PIPE

from io import BytesIO


from threading import Thread
import functools

from pinger import logfile_path, domains_file, savefolder

AMPLIFIER = 2

def timeout(timeout):
    def deco(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            res = [Exception('function [%s] timeout [%s seconds] exceeded!' % (func.__name__, timeout))]
            def newFunc():
                try:
                    res[0] = func(*args, **kwargs)
                except Exception as e:
                    res[0] = e
            t = Thread(target=newFunc)
            t.daemon = True
            try:
                t.start()
                t.join(timeout)
            except Exception as je:
                print ('error starting thread')
                raise je
            ret = res[0]
            if isinstance(ret, BaseException):
                raise ret
            return ret
        return wrapper
    return deco


@timeout(10*AMPLIFIER)
def process_one(domain):
    windows_cnt = len(driver.window_handles)
    driver.execute_script('''window.open("https://%s", "_blank");'''%domain)
    driver.switch_to.window(driver.window_handles[-1])
    windows_cnt_after = len(driver.window_handles)
    if windows_cnt_after == windows_cnt:
        raise AssertionError('script execution is blocked by previous url!')
    t.sleep(3)
    for _ in range(int(5*AMPLIFIER)):
        t.sleep(1)
        flag = driver.execute_script('return document.readyState === "complete"')
        print(flag)
        if flag:
            t.sleep(1)
            break
    driver.execute_script("window.stop()")
    print(driver.current_url)
    if driver.current_url == 'about:blank':
        raise ValueError('url %s is unreachable'%domain)
    b = BytesIO(driver.get_screenshot_as_png())
    b.seek(0)
    return b


def load_ping_flags(k0):
    ping_filter = dict(last=k0)
    with open(logfile_path, 'r') as f:
        for k, _, flag in map(str.split, map(str.strip, f)):
            if int(k) < k0: continue
            ping_filter[int(k)] = int(flag)
            ping_filter['last'] = int(k)
    return ping_filter


if __name__ == '__main__':
    caps = DesiredCapabilities().CHROME
    caps["pageLoadStrategy"] = "none"   # Do not wait for full page load
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--ignore-ssl-errors')
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--user-data-dir=tmp")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disk-cache-size=0")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    driver = webdriver.Chrome(executable_path='chromedriver.exe',
                              chrome_options=chrome_options,
                              desired_capabilities=caps)
    driver.delete_all_cookies()

    data = []
    labels = []
    html = []

    CHUNK_SIZE = 50
    os.makedirs(savefolder, exist_ok=True)

    domain_df = pd.read_csv(domains_file, sep='\t')
    try:
        offset = sorted([int(x.split('.')[0]) for x in os.listdir(savefolder)])[-1]
    except IndexError:
        offset = 0


    ping_filter = load_ping_flags(offset)
    for i, domain in enumerate(domain_df.domain.tolist()):
        if 'lordfilm' in domain or 'lordsfilm' in domain: continue
        if i < offset: continue
        if i > ping_filter['last']:
            ping_filter = load_ping_flags(i)
        print(domain)
        if ping_filter.get(i) == 0: continue
        try:
            b = process_one(domain)
        except Exception as e:
            print(i, domain, e)
            with gzip.open(os.path.join(savefolder, '%d.pickle.gz'%(i+1)), 'wb') as fb:
                pickle.dump(dict(data=np.uint8(data),
                                 labels=np.array(labels),
                                 html=np.array(html)), fb, protocol=-1)
            exit()
        data.append(np.uint8(Image.open(b).convert('RGB').resize((600, 314))))
        del b
        labels.append((domain, driver.title))
        html.append(str(driver.page_source))

        print(len(data))
        if (i+1) % CHUNK_SIZE == 0:
            with gzip.open(os.path.join(savefolder, '%d.pickle.gz'%(i+1)), 'wb') as fb:
                pickle.dump(dict(data=np.uint8(data),
                                 labels=np.array(labels),
                                 html=np.array(html)), fb, protocol=-1)
            driver.close()
            driver.quit()
            driver = webdriver.Chrome(executable_path='chromedriver.exe',
                              chrome_options=chrome_options,
                              desired_capabilities=caps)
            data = []
            labels = []
            html = []
    if len(data):
        with gzip.open(os.path.join(savefolder, '%d.pickle.gz'%(i+1)), 'wb') as f:
            pickle.dump(dict(data=np.uint8(data),
                             labels=np.array(labels),
                             html=np.array(html)), f, protocol=-1)
