import csv
import requests
import time
import os
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool

file = open("fingridFrekvensHistorik.csv", "r")
urls = list(csv.reader(file, delimiter=","))
file.close()

fns = []
for url in urls:
    parentdir = 'files'

    if not os.path.exists(parentdir):
        os.makedirs(parentdir)

    name = url[0].split('/')[-1]
    fn = fr'.\{parentdir}\{name}'
    fns.append(fn)

inputs = zip(urls, fns)


def download_url(args):
    t0 = time.time()
    url, fn = args[0], args[1]
    try:
        r = requests.get(url[0])
        with open(fn, 'wb') as f:
            f.write(r.content)
            return url, time.time() - t0
    except Exception as e:
        print('Exception in download_url():', e)


def download_parallel(args):
    cpus = cpu_count()
    results = ThreadPool(cpus - 1).imap_unordered(download_url, args)
    for result in results:
        if result is not None:
            print('url:', result[0][0], 'time (s):', result[1])


download_parallel(inputs)
