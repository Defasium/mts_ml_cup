import os
import time
import stat
from subprocess import Popen
from shutil import rmtree

if __name__=='__main__':
    Popen(['python', 'pinger.py'])
    while True:
        try:
            Popen(['python', 'worker.py']).wait()
        finally:
            for _ in range(3):
                try:
                    os.system("taskkill /im chrome.exe")
                    time.sleep(1)
                except: pass
            pass
        for _ in range(3):
            try:
                os.system("rm -r tmp")
            finally:
                pass
