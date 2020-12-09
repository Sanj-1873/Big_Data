
import socket
from _thread import *
import threading
import json
import sys
import random
import operator
import os
from datetime import datetime
from collections import defaultdict

global availableSlots
global tasks_queue
global map_tasks_count
global tasks_queue
global red_tasks
global job_tasks_count
global availableSlots
global map_tasks_count
global job_tasks_count



class myThread(threading.Thread):
   #set initial values
    def __init__(self, threadID, func):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.func = func
    def run(self):
        self.func()

def RANDOM():
    while 1:
        worker_id = random.randint(1, 3)
        test_condition = availableSlots[worker_id]['slots']
        if  test_condition <= 0:
                continue
        else:
                return worker_id

def RR():
    while 1:
        for worker_id in range(1,4):
            test_condition = availableSlots[worker_id]['slots']
            if  test_condition <= 0:
                continue
            else:
                return worker_id

def LL():
    while 1:
        test_condition = max(range(1,4),key=lambda x: availableSlots[x]['slots'])
        if test_condition:
            return test_condition



#allocate tasks to the right worker     
def scheduler():
    functions = {"RANDOM":RANDOM,"RR":RR,"LL":LL}
    scheduling_algo = sys.argv[2]
    for i in iter(int,1):
        taskQueueSize = len(tasks_queue)
        if(taskQueueSize < 0):
            return
        for task in tasks_queue:
            try:
                worker_id = functions[scheduling_algo]()
            except:
                print("Invalid Input")
                return
            port = availableSlots[worker_id]['port']
            task = tasks_queue.pop(0)
            task_message = json.dumps(task)
            try: 
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(("localhost", port))
                try:
                    f = open("log/master.txt", "a+")
                    log_lock.acquire()
                    print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tCONNECTION OUTGOING = [host:{'localhost'}, port:{port}]",file=f)
                    log_lock.release()
                except:
                    print("UNKNOWN ERROR OCCURED DURING LOGGING")
                else:
                    f.close()
                s.send(task_message.encode())
            except Exception as e:
                try:
                    f = open("log/master.txt", "a+")
                    log_lock.acquire()
                    print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tERROR OCCURED [error_message: {e}]",file=f)
                    log_lock.release()
                except:
                    print("UNKNOWN ERROR OCCURED DURING LOGGING")
                else:
                    f.close()
            try:
                f = open("log/master.txt", "a+")
                log_lock.acquire()
                print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tTASK SENT TO = [worker_id:{worker_id}, task_id:{task['task_id']}, job_id:{task['job_id']}, duration:{task['duration']}]",file=f)
                log_lock.release()
            except Exception as e:
                print(f"UNKNOWN ERROR OCCURED DURING LOGGING = [error_message:{e}]")
            else:
                f.close()
            print(f"Sent task {task['task_id']} to worker {worker_id} on port {port}")
            worker_lock.acquire()
            availableSlots[worker_id]['slots'] -= 1
            worker_lock.release()


def getRequests():
    host = ""
    port = 5000
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((host, port))
        s.listen(1000)
        while(True):
            conn, addr = s.accept()
            try:
                f = open("log/master.txt", "a+")
                log_lock.acquire()
                print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tCONNECTION INCOMING = [host:{addr[0]}, port:{addr[1]}]",file=f)
                log_lock.release()
            except Exception as e:
                print(f"UNKNOWN ERROR OCCURED DURING LOGGING = [error_message:{e}]")
            else:
                f.close()
            req = json.loads(conn.recv(100000))
            task_list={"map":[m_t['task_id'] for m_t in req['map_tasks']],"reduce":[r_t['task_id'] for r_t in req['reduce_tasks']]}
            try: 
                f = open("log/master.txt", "a+")
                log_lock.acquire()
                print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tRECEIVED JOB REQUEST = [job_id:{req['job_id']}, map_tasks_ids:{task_list['map']}, reduce_tasks_ids:{task_list['reduce']}]",file=f)
                log_lock.release()
            except Exception as e:
                print(f"UNKNOWN ERROR OCCURED DURING LOGGING = [error_message:{e}]")
            else:
                f.close()
            no_map_tasks = len(req['map_tasks'])
            map_tasks_count[req['job_id']] = no_map_tasks
            job_tasks_count[req['job_id']] = len(req['map_tasks']) + len(req['reduce_tasks'])
            red_tasks[req['job_id']] = list()
            task = dict()
            for i in req['reduce_tasks']:
                task[i['task_id']] = dict()
                task[i['task_id']]['task_id'] = i['task_id'] 
                task[i['task_id']]['job_id'] = req['job_id']
                task[i['task_id']]['duration'] = i['duration']
                red_tasks[req['job_id']].append(task[i['task_id']])
            task = dict()
            for i in req['map_tasks']:
                task[i['task_id']] = dict()
                task[i['task_id']]['task_id'] = i['task_id'] 
                task[i['task_id']]['job_id'] = req['job_id']
                task[i['task_id']]['duration'] = i['duration']
                tasks_queue.append(task[i['task_id']])
    except Exception as e:
        try: 
            f = open("log/master.txt", "a+")
            log_lock.acquire()
            print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tERROR OCCURED [error_message: {e}]",file=f)
            log_lock.release()
        except Exception as e:
            print(f"UNKNOWN ERROR OCCURED DURING LOGGING = [error_message:{e}]")
        else:
            f.close()
    else:
        s.close()


def getUpdatesWorkers():
    host = ""
    port = 5001
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((host, port))
        s.listen(1000)
        while(True):
            try:
                conn, addr = s.accept()
                try:
                    f = open("log/master.txt", "a+")
                    log_lock.acquire()
                    print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tCONNECTION INCOMING = [host:{addr[0]}, port:{addr[1]}]",file=f)
                    log_lock.release()
                except Exception as e:
                    print(f"UNKNOWN ERROR OCCURED DURING LOGGING = [error_message:{e}]")
                else:
                    f.close()
                update_json = conn.recv(1000)
                update = json.loads(update_json)
                worker_id = update['worker_id']
                job_id = update['job_id']
                task_id = update['task_id']
                try:
                    f = open("log/master.txt", "a+")
                    log_lock.acquire()
                    print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tTASK COMPLETED AT = [worker_id:{worker_id}, task_id:{task_id}]",file=f)
                    log_lock.release()
                except Exception as e:
                    print(f"UNKNOWN ERROR OCCURED DURING LOGGING = [error_message:{e}]")
                else:
                    f.close()
                print(f"Task {task_id} completed execution in {worker_id}")
                job_tasks_count[job_id] -= 1
                if job_tasks_count[job_id] == 0:
                    try:
                        f = open("log/master.txt", "a+")
                        log_lock.acquire()
                        print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tFINISHED EXECUTING JOB = [job_id:{job_id}]",file=f)
                        log_lock.release()
                    except Exception as e:
                        print(f"UNKNOWN ERROR OCCURED DURING LOGGING = [error_message:{e}]")
                    else:
                        f.close()
                worker_lock.acquire()
                availableSlots[worker_id]['slots'] += 1
                worker_lock.release()
                if 'M' in task_id:
                    map_tasks_count[job_id] -= 1
                    if map_tasks_count[job_id] == 0:
                        for reduce_task in red_tasks[job_id]:
                            tasks_queue.append(reduce_task)
            except Exception as e:
                try: 
                    f = open("log/master.txt", "a+")
                    log_lock.acquire()
                    print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tERROR OCCURED [error_message: {e}]",file=f)
                    log_lock.release()
                except Exception as e:
                    print(f"UNKNOWN ERROR OCCURED DURING LOGGING = [error_message:{e}]")
                else:
                    f.close()
            else:
                conn.close()
    except Exception as e:
        try: 
            f = open("log/master.txt", "a+")
            log_lock.acquire()
            print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tERROR OCCURED [error_message: {e}]",file=f)
            log_lock.release()
        except Exception as e:
            print(f"UNKNOWN ERROR OCCURED DURING LOGGING = [error_message:{e}]")
        else:
            f.close()
    else:
        s.close()


map_tasks_count = dict()
job_tasks_count = dict()
red_tasks = dict()
tasks_queue = list()
worker_lock = threading.Lock()
taskLock = threading.Lock()
log_lock = threading.Lock()


if not(os.path.isdir('log')):
    os.mkdir('log')

open("log/master.txt", "w").close()


try:
    f = open(sys.argv[1])
    config = json.load(f)
except:
    pass
else:
    f.close()

if verbose:
    print(config)

availableSlots = dict()
for worker in config['workers']:
    availableSlots[worker['worker_id']] = {'slots':worker['slots'], 'port':worker['port']}

print(availableSlots)

request_thread = myThread(1, getRequests)
workers_thread = myThread(2, getUpdatesWorkers)
schedule_thread = myThread(3, scheduler)
request_thread.start()
workers_thread.start()
schedule_thread.start()
request_thread.join()
workers_thread.join()
schedule_thread.join()