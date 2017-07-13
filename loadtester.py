#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Quick and dirty multithreaded / multicore load tester for REST APIs.

TODO: Support request bodies.
"""

__author__ = 'Adam Rafuse <$(echo nqnz.enshfr#tznvy.pbz | tr a-z# n-za-m@)>'

import os
import sys
import argparse
import json
import threading
import time
import queue
import random
import copy
import multiprocessing

sys.path.insert(0, 'lib/')
import requests

# Application defaults.
NUM_WORKERS = 5000                               # Total number test workers
NUM_PROCESSES = multiprocessing.cpu_count() + 1  # Number of processes (cores) to utilize
THREAD_SLEEP = 0.1

# Global timer used by each process.
global_timer = time.time()

def main(config_filename, num_workers, num_processes, output_filename, output_format):
    start_load_test = multiprocessing.Event()
    process_output = multiprocessing.Queue()
    process_data = multiprocessing.Queue()                        
    output_file = open(output_filename, 'w') if output_filename is not None else None
            
    # Load and output configuration data.
    with open(config_filename) as config_file:
        config = json.load(config_file)        
    print(json.dumps(config))
    if output_file is not None and output_format is None:
        output_file.write(json.dumps(config) + '\n')
            
    # Spawn load test processes.
    processes = [ Dispatcher(num, start_load_test, process_output, process_data, config,
                             num_workers // num_processes) for num in range(num_processes) ] 
    
    # Start each process which will stand by once they've kicked up all their workers.   
    for process in processes:
        process.start()       
    join_and_output(num_processes, process_output, output_file if output_format is None else None)
    
    # Unleash the damage.
    start_load_test.set()
    join_and_output(num_processes, process_output, output_file if output_format is None else None)
    
    # Output CSV from worker data if requested.
    if output_format == 'csv':
        output_csv(process_data, output_file)
            
    # Close output file if specified.
    if output_file is not None:
        output_file.close()        

def join_and_output(num, queue, output_file):
    """
    Joins on a queue while directing output from it. The queue is assumed to be filled by 'num' threads / processes
    and interprets 'None' in the queue as a signal from one of the threads / processes that it has completed. Will
    print to standard output and optionally write to 'out_file' file object if it is not 'None'.
    """
    count = num
    while count:
        output = queue.get(block=True)
        if output is None:
            count -= 1
        else:
            print(output)
            if output_file is not None:
                output_file.write(output + "\n")
    
def output_csv(queue, output_file):
    """
    Outputs data from a queue to a file object in CSV format. Each queue entry is expected to contain a list of
    primitives.     
    """
    while not queue.empty():
        data = queue.get()
        output_file.write(str(data[0]))
        for element in data[1:]:
            output_file.write(',' + str(element))
        output_file.write('\n')
    
class Dispatcher(multiprocessing.Process):
    """
    Process which controls execution of Requester threads.
    """
    
    def __init__(self, process_id, ready, output, data, config, num_workers, *args, **kwargs):
        multiprocessing.Process.__init__(self, *args, **kwargs)
        self.process_id = process_id
        self.ready = ready
        self.output = output
        self.data = data
        self.config = config
        self.num_workers = num_workers
        
    def run(self):
        threads_completed = queue.Queue()
        
        # Kick off all worker threads, which will wait before making any requests.
        start_workers = threading.Event()
        workers = [ Requester(num, start_workers, threads_completed, self.config)
                    for num in range(self.num_workers)]
        for i in range(self.num_workers):
            workers[i].start()
            self.output.put("Process {} initialized worker {}".format(self.process_id, i))
        
        # Stand by and wait until the calling process tells us to begin.
        self.output.put(None)
        self.ready.wait()
        
        # Reset the global timer and signal workers to begin making requests.
        global global_timer
        self.output.put("Process {} beginning load test at {:.5f}s".format(self.process_id,
                                                                            time.time() - global_timer))
        global_timer = time.time()
        start_workers.set()
                        
        # Join on the queue of completed worker statuses while aggregating the data from them in order.
        worker_count = self.num_workers
        while (worker_count):
            try:
                worker_id = threads_completed.get()                
                worker_count -= 1
                
                # Set a globally unique id for each worker based on process id.                                          
                workers[worker_id].data[0] = (workers[worker_id].data[0] + 1) * (self.process_id + 1) - 1
                                
                # Send output and data back to the calling process.
                self.output.put('Process {}, {}'.format(self.process_id, workers[worker_id].info))
                self.data.put(workers[worker_id].data)
                
            except queue.Empty:                
                time.sleep(THREAD_SLEEP)
                
        self.output.put(None)
                  
class Requester(threading.Thread):
    """
    Thread which handles the request and retry logic for a single load test worker.
    """
    
    def __init__(self, worker_id, ready, completed, config, *args, **kwargs):
        threading.Thread.__init__(self, *args, **kwargs)
        self.worker_id = worker_id
        self.ready = ready
        self.completed = completed
        self.config = config
        self.status = 0
        self.info = 'worker {}:'.format(self.worker_id)
        self.data = [worker_id]
        
        # Disable automatic retries because we handle them ourselves.
        self.session = requests.Session()
        self.session.mount('http://', requests.adapters.HTTPAdapter(max_retries=0))
        self.session.mount('https://', requests.adapters.HTTPAdapter(max_retries=0))
        
    def run(self):                
        method = self.config['method']
        url = self.config['url']
        params = self.config['params'].copy()  # Copy to avoid repeatedly clobbering parameters
        randomize = self.config['randomize']
        retries = 0 
        
        # Pick a random value for the parameters we want randomized. Randomized parameters are defined in groups
        # where each member gets the same random index, so they can be linked (eg. usernames with passwords). This
        # means the value list for parameters in the same group must be the same size.
        if randomize:
            for group in randomize:            
                index = random.randint(0, len(params[group[0]]) - 1)
                for entry in group:                    
                    params[entry] = params[entry][index]
                    self.info += "\n  Selected value '{}' for randomized parameter '{}'".format(params[entry], entry)
                                                    
        # Wait until the calling thread tells us to begin.
        self.ready.wait()
        
        # Begin handling the request.
        start_time = time.time()
        elapsed_time = start_time - global_timer
        self.info += '\n  Sending request at {:.5f}s'.format(elapsed_time) 
        self.data.append(elapsed_time)
                                         
        while self.status != 200:
            try:                            
                response = self.session.request(method, url, timeout=self.config['timeout'], stream=True)
                self.status = response.status_code 
                
                if (self.status == 200):
                    elapsed_time = time.time() - start_time
                    self.info += '\n  Got 200 response at {:+.5f}s'.format(elapsed_time)
                    self.data.extend([200, elapsed_time])
                
                elif ((self.status  >= 500 and self.status <= 599) or (self.status in [400, 408, 429])):
                    elapsed_time = time.time() - start_time
                    self.info += '\n  Retrying for status {} at {:+.5f}s'.format(self.status, elapsed_time)
                    self.data.extend([self.status, elapsed_time])                    
                    retries += 1;
                    time.sleep(2 ** retries)
                
                else:
                    elapsed_time = time.time() - start_time
                    self.info += '\n  Got non-retryable status {} at {:+.5f}s'.format(self.status, elapsed_time)
                    self.data.extend([self.status, elapsed_time])
                    break
                
            except Exception as ex:
                elapsed_time = time.time() - start_time
                self.info += '\n  Retrying for {} at {:+.5f}s'.format(type(ex).__name__, elapsed_time)
                self.data.extend([type(ex).__name__, elapsed_time])
                retries += 1;
                time.sleep(2 ** retries)
        
        # Clean up and signal completion to the calling thread.
        response.close()        
        self.completed.put(self.worker_id)    
                                    
if __name__ == '__main__':
    argParser = argparse.ArgumentParser(description='Quick and dirty load tester for HTTPS REST APIs.')
    argParser.add_argument('config', help='Path to JSON configuration file.')
    argParser.add_argument('-w', '--workers', type=int,
                           help="Total number of test workers to spawn (default {}).".format(NUM_WORKERS))
    argParser.add_argument('-p', '--processes', type=int,
                           help="Total number of processes to use (default {}).".format(NUM_PROCESSES))
    argParser.add_argument('-o', '--output', type=str,
                           help="Optional output file.")
    argParser.add_argument('-f', '--format', type=str,
                           help="Output file format. Either 'csv' or not specified (same as standard output).")
    args = argParser.parse_args()

    if not (os.path.isfile(args.config)):
        sys.stderr.write("Configuration file '{}' not found.\n\n".format(args.output))

    main(args.config,
         args.workers if args.workers else NUM_WORKERS,
         args.processes if args.processes else NUM_PROCESSES,
         args.output if args.output else None,
         args.format.lower() if args.format else None)
