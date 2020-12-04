
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
global taskQueue
global mapTaskCounts
global taskQueue
global reduceTasks
global jobTaskCounts
global availableSlots
global mapTaskCounts
global jobTaskCounts



class myThread(threading.Thread):
   #set initial values
    def __init__(self, threadID, func):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.func = func
    
    def run(self):
        self.func()

def RANDOM():
    for i in iter(int,1):
        worker_id = random.randint(1, 3)
        test_condition = availableSlots[worker_id]['slots']
        if  test_condition < 0:
                pass
        else:
                return worker_id

def RR():
    for i in iter(int,1):
        for worker_id in range(1,4):
            test_condition = availableSlots[worker_id]['slots']
            if  test_condition < 0:
                pass
            else:
                return worker_id

def LL():
    while:
        x= max(range(1,4),key=lambda x: availableSlots[x]['slots'])
        if x!=None:
            return x

functions = {"RANDOM":RANDOM,"RR":RR,"LL":LL}

#allocate tasks to the right worker     
def scheduler(): 
    scheduling_algo = sys.argv[2]
    for i in iter(int,1):
        taskQueueSize = len(taskQueue)
        if(taskQueueSize < 0):
            pass
        else:
            for task in taskQueue:
                try:
                     worker_id = functions[scheduling_algo]()
                except:
                    print("Invalid Input")
                    return
                
                #picking the correct worker 
                port = availableSlots[worker_id]['port']
                task = taskQueue[0]
                taskQueue.remove(task)
                task_message = json.dumps(task)
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(("localhost", port))
                
                    f = open("log/master.txt", "a+")
                    #with open("log/master.txt", "a+") as f:
                    try:
                        printLock.acquire()
                        print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tCONNECTION OUTGOING = [host:{'localhost'}, port:{port}]",file=f)
                        printLock.release()
                    finally:
                        f.close()

                    s.send(task_message.encode())
            


                
                f = open("log/master.txt", "a+")
                try:
                    printLock.acquire()
                    print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tTASK SENT TO = [worker_id:{worker_id}, task_id:{task['task_id']}, job_id:{task['job_id']}, duration:{task['duration']}]",file=f)
                    printLock.release()
                finally:
                    f.close()

                print(f"Sent task {task['task_id']} to worker {worker_id} on port {port}")
                
                workerLock.acquire()
                availableSlots[worker_id]['slots'] -= 1
                workerLock.release()
    
       
#listen for requests
def getRequests():
    host = ""
    port = 5000
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    
    with s:
        s.bind((host, port))
        s.listen(1000)
        while(True):
            c, addr = s.accept()
            with open("log/master.txt", "a+") as f:
                printLock.acquire()
                print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tCONNECTION INCOMING = [host:{addr[0]}, port:{addr[1]}]",file=f)
                printLock.release()
            with c:
                req = c.recv(100000)
                req = json.loads(req)
                task_list={"map":[m_t['task_id'] for m_t in req['map_tasks']],"reduce":[r_t['task_id'] for r_t in req['reduce_tasks']]}
                with open("log/master.txt", "a+") as f:
                    printLock.acquire()
                    print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tRECEIVED JOB REQUEST = [job_id:{req['job_id']}, map_tasks_ids:{task_list['map']}, reduce_tasks_ids:{task_list['reduce']}]",file=f)
                    printLock.release()
                no_map_tasks = len(req['map_tasks'])
                mapTaskCounts[req['job_id']] = no_map_tasks
                jobTaskCounts[req['job_id']] = len(req['map_tasks']) + len(req['reduce_tasks'])
                reduceTasks[req['job_id']] = []
                tasks = req['reduce_tasks']
                task = {}
                for i in tasks:
                    task[i['task_id']] = {}
                    task[i['task_id']]['task_id'] = i['task_id'] 
                    task[i['task_id']]['job_id'] = req['job_id']
                    task[i['task_id']]['duration'] = i['duration']
                    reduceTasks[req['job_id']].append(task[i['task_id']])
                    


                tasks = req['map_tasks']
                task = {}
                for i in tasks:
                    task[i['task_id']] = {}
                    task[i['task_id']]['task_id'] = i['task_id'] 
                    task[i['task_id']]['job_id'] = req['job_id']
                    task[i['task_id']]['duration'] = i['duration']
                    taskQueue.append(task[i['task_id']])
                

def getUpdatesWorkers():
    
    host = ""
    port = 5001
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with s:
        s.bind((host, port))
        s.listen(1000)
        while(True):
            c, addr = s.accept()
            with open("log/master.txt", "a+") as f:
                printLock.acquire()
                print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tCONNECTION INCOMING = [host:{addr[0]}, port:{addr[1]}]",file=f)
                printLock.release()
            with c:
                update_json = c.recv(1000)
                update = json.loads(update_json)
                worker_id = update['worker_id']
                job_id = update['job_id']
                task_id = update['task_id']
                with open("log/master.txt", "a+") as f:
                    printLock.acquire()
                    print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\t TASK COMPLETED AT = [worker_id:{worker_id}, task_id:{task_id}]",file=f)
                    printLock.release()
                print(f"Task {task_id} completed execution in {worker_id}")
                jobTaskCounts[job_id] -= 1
                if jobTaskCounts[job_id] == 0:
                    with open("log/master.txt", "a+") as f:
                        printLock.acquire()
                        print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tFINISHED EXECUTING JOB = [job_id:{job_id}]",file=f)
                        printLock.release()
                workerLock.acquire()
                availableSlots[worker_id]['slots'] += 1
                workerLock.release()
                
                if 'M' in task_id: #map job is completed
                    mapTaskCounts[job_id] -= 1
                    if mapTaskCounts[job_id] == 0:
                        for reduce_task in reduceTasks[job_id]:
                            taskQueue.append(reduce_task)
                        
                    

mapTaskCounts = {} #number of map tasks of every job remaining to be executed
jobTaskCounts = {} #total number of tasks in every job remaining to be executed
reduceTasks = {} #list of reduce tasks for every job
taskQueue = [] #list of tasks to be scheduled
workerLock = threading.Lock()
taskLock = threading.Lock()
printLock = threading.Lock()


#make a new dir for the logs
try:
    os.mkdir('log')
#Skip if already present
except:
    pass 

open("log/master.txt", "w").close()

path_to_config = sys.argv[1]
with open(path_to_config) as f:
    config = json.load(f)
#print(config)

availableSlots = {} 
for worker in config['workers']:
    availableSlots[worker['worker_id']] = {'slots':worker['slots'], 'port':worker['port']}

print(availableSlots)
thread1 = myThread(1, getRequests)
thread2 = myThread(2, getUpdatesWorkers)
thread3 = myThread(3, scheduler)
thread1.start()
thread2.start()
thread3.start()
thread1.join()
thread2.join()
thread3.join()