#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import re
from datetime import datetime
from urllib.request import urlopen, urlretrieve
from urllib.parse import urlparse
from urllib.error import HTTPError, URLError
from bs4 import BeautifulSoup

import logging
import threading
from copy import deepcopy
from timeit import default_timer
from queue import Queue
from collections import defaultdict


logger = logging.getLogger(__name__)

all_shops = None
prog = None
prog_before = None
prog_max = None

def make_shoplist():
    global all_shops, prog, prog_before, prog_max
    with open('get_review_progress.txt', 'r') as f:
        lines = f.read().splitlines()
    prog = {}
    prog_max = {}
    for line in lines:
        val, pref = line.split()
        prog[pref] = int(val)
    prog_before = deepcopy(prog)
    with open('shops_all.txt', 'r') as f:
        lines = f.read().splitlines()
    all_shops = {}
    for pref, val in prog.items():
        tmp = [s.split()[0] for s in lines if s.startswith('/' + pref)]
        prog_max[pref] = len(tmp)
        all_shops[pref] = tmp[val:]
        logging.info('making shop list of %11s: %6i of %6i (%.1f%%) shops done.',
                     pref, val, len(tmp), 100.0 * val / len(tmp))

url_fmt = 'https://tabelog.com{}dtlrvwlst/COND-0/smp1/?lc=0&rvw_part=all&PG={}'

def _do_get_review(job):
    pref, path = job
    for pg in range(1, 100):
        try:
            html = urlopen(url_fmt.format(path, pg))
        except HTTPError:
            return (1, pref, path, pg-1)
        except (URLError, ConnectionResetError):
            return (0, pref, path, pg-1)
        else:
            bsObj = BeautifulSoup(html, 'html.parser')
            if 'アクセスが制限' in bsObj.find('h1').get_text():
                return (0, pref, path, pg)
            result = set()
            for link in bsObj.findAll('a', href=re.compile('^(/|http://tabelog.com)')):
                if link['href'].startswith('//'):
                    continue
                o = urlparse(link['href'])
                if o.path not in result and re.match('.*/dtlrvwlst/\d+/$', o.path):
                    result.add(o.path)
            for r in sorted(result):
                try:
                    urlretrieve('http://tabelog.com' + r,
                                pref + '/' + r.replace('/', '_') + '.html')
                    if os.path.getsize(pref + '/' + r.replace('/', '_') + '.html') < 10000:
                        return (0, pref, path, pg)
                    print(r)
                except HTTPError:
                    continue
                except (URLError, ConnectionResetError):
                    return (0, pref, path, pg)


def get_review(num_worker=16, report_delay=1.0):
    global prog
    
    def worker_loop():
        while True:
            job = job_queue.get()
            if job is None:
                progress_queue.put(None)
                break
            stat, pref, path, page = _do_get_review(job)
            progress_queue.put((stat, pref, path, page))
            
    def job_producer():
        nonlocal total_count
        pref_no = 0
        for key, value in all_shops.items():
            if not value:
                logger.info('%s prefecture already done.', key)
                continue
            if key not in os.listdir():
                os.mkdir(key)
            pref_no += 1
            for target in value:
                total_count += 1
                job_queue.put((key, target))
        for _ in range(num_worker):
            job_queue.put(None)
        logger.info("job loop exiting, total %i jobs in %i prefs", total_count, pref_no)

    job_queue = Queue()
    progress_queue = Queue()
    total_count, finished_count, finished_total_count = 0, 0, 0

    workers = [threading.Thread(target=worker_loop) for _ in range(num_worker)]
    unfinished_worker_count = len(workers)
    workers.append(threading.Thread(target=job_producer))
    
    for thread in workers:
        thread.daemon = True
        thread.start()

    start, next_report = default_timer() - 0.00001, 3.0

    while unfinished_worker_count > 0:
        report = progress_queue.get()
        if report is None:
            unfinished_worker_count -= 1
            logger.info("worker thread finished; "
                        "awaiting finish of %i more threads", unfinished_worker_count)
            continue
        stat, pref, path, page = report
        if not stat:
            logger.info('access restricted, and output result')
            break

        prog[pref] += stat * 1
        finished_count += stat * 1
        finished_total_count += page

        # log progress
        elapsed = default_timer() - start
        if elapsed >= next_report:
            logger.info('PROGRESS: at %.2f%% of jobs, %.0f speed, %i of %i done at %s.',
                        100.0 * finished_count / total_count,
                        finished_total_count / elapsed,
                        finished_count, total_count,
                        pref)
            
            next_report = elapsed + report_delay
    else:
        logger.info('done!')
    output_result()

    
def output_result():
    for k, v in prog.items():
        logger.info('%11s: %6i >> %6i / %6i', k, prog_before[k], v, prog_max[k])
    with open('get_review_progress_old.txt', 'w') as f_before:
        for k, v in prog_before.items():
            f_before.write('{} {}\n'.format(v, k))
    with open('get_review_progress.txt', 'w') as f_after:
        for k, v in prog.items():
            f_after.write('{} {}\n'.format(v, k))

    
        
if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s : %(threadName)s : %(levelname)s : %(message)s',
        level=logging.INFO)
    logging.info("running %s", " ".join(sys.argv))
    make_shoplist()
    try:
        get_review()
    except KeyboardInterrupt:
        output_result()

