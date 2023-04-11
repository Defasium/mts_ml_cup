import time as t
import os
import pandas as pd
from subprocess import Popen, PIPE
from multiprocessing.dummy import Pool as ThreadPool

def ping(args):
    i, domain = args
    out, _ = Popen(['ping', '-w', '50', domain], stdout=PIPE).communicate()
    return i, domain, int(str(out).count(' = 4,')==2)

batch = []
CHUNK_SIZE = 32
savefolder = 'desktop_chunks20k'
logfile_path = 'ping_info20k.log'
domains_file = "top20kdomains.gz"

if __name__=='__main__':
    domain_df = pd.read_csv(domains_file, sep='\t')
    os.makedirs(savefolder, exist_ok=True)

    offset = 0

    if os.path.exists(logfile_path):
        with open(logfile_path, 'r') as f:
            for k, _, _ in map(str.split, map(str.strip, f)):
                offset = max(int(k)+1, offset)

    for i, domain in enumerate(domain_df.domain.tolist()):
        if 'lordfilm' in domain or 'lordsfilm' in domain: continue
        if i < offset: continue
        batch.append((i, domain))
        if len(batch) % CHUNK_SIZE == 0:
            with ThreadPool(8) as tpool, open(logfile_path, 'a') as f:
                for i, domain, res in tpool.map(ping, batch):
                    f.write('%d\t%s\t%d\n'%(i, domain, res))
            batch = []

    if len(batch):
        with ThreadPool(8) as tpool, open(logfile_path, 'a') as f:
            for i, domain, res in tpool.map(ping, batch):
                f.write('%d\t%s\t%d\n'%(i, domain, res))
