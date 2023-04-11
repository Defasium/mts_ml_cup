import os
import time
import stat
from subprocess import Popen
from shutil import rmtree
from multiprocessing.pool import Pool

def subprocess_one(args):
    i, k = args
    Popen(['python', 'simwebsearch.py',
           '-k', str(k), '-i', str(i),
           '--num_workers', str(NUM_WORKERS),
           '--savefolder', SAVEFOLDER,
           '--url_file', URL_FILE]).wait()


NUM_WORKERS = 3
SAVEFOLDER = 'simwebhtml'
URL_FILE = 'urls.txt'
if __name__=='__main__':

    with Pool(NUM_WORKERS) as pool:
        while True:
            print(i)
            try:
                pool.map(subprocess_one, [(i, k) for k in range(NUM_WORKERS)])
            finally:
                for _ in range(3):
                    try:
                        os.system("taskkill /im chrome.exe")
                        time.sleep(1)
                    except: pass
                pass
            for _ in range(3):
                try:
                    pass
                finally:
                    pass
