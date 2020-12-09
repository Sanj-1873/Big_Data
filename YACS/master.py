from collections import defaultdict
from datetime import datetime
import json
import operator
import os
import random
import socket
import sys
from _thread import *
import threading

global availableSlots
global jobTaskCounts
global mapTaskCounts
global reduceTasks
global taskQueue
    
#jobs that need to be executed
jobTaskCounts = dict() 

#map tasks that need to be executed
mapTaskCounts = dict()

#reduce tasks that need to be executed
reduceTasks = dict() 

#tasks that need to be sechduled 
taskQueue = list()

#prevent race conditions 
workerLock = threading.Lock()
taskLock = threading.Lock()
printLock = threading.Lock()



class myThread(threading.Thread):
   #set initial values
    def __init__(self, threadID, func):
        threading.Thread.__init__(self)
        self.func = func
        self.threadID = threadID
        
    
    def run(self):
        self.func()

#assuming there are 3 workers 
def RR():
    for i in iter(int,1):  
        for worker_id in range(1,4):
            test_condition = availableSlots[worker_id]['slots']
            if  test_condition < 0:
                pass
            else:
                return worker_id

def LL():
    while 1:
        test_condition = max(range(1,4),key=lambda x: availableSlots[x]['slots'])
        if test_condition:
            return test_condition

def RANDOM():
    for i in iter(int,1):
        worker_id = random.randint(1, 3)
        test_condition = availableSlots[worker_id]['slots']
        if  test_condition < 0  :
                pass
        else:
                return worker_id





#listen for requests
def Requests():
    host = ""
    port = 5000

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((host, port))
        s.listen(1000)
        for i in iter(int,1):
            
            try:
                c, addr = s.accept()
                f = open("log/master.txt", "a+")
                try:
                    printLock.acquire()
                    print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tCONNECTION INCOMING = [host:{addr[0]}, port:{addr[1]}]",file=f)
                    printLock.release()
                except:
                    print("UNKNOWN ERROR OCCURED DURING LOGGING")
                else:
                    f.close()

                req = c.recv(100000)
                req = json.loads(req)
                task_list={"map":[m_t['task_id'] for m_t in req['map_tasks']],"reduce":[r_t['task_id'] for r_t in req['reduce_tasks']]}
                f = open("log/master.txt", "a+")
                try:
                    printLock.acquire()
                    print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tRECEIVED JOB REQUEST = [job_id:{req['job_id']}, map_tasks_ids:{task_list['map']}, reduce_tasks_ids:{task_list['reduce']}]",file=f)
                    printLock.release()
                except:
                    print("UNKNOWN ERROR OCCURED DURING LOGGING")
                else:
                    f.close()
                
                num_map_tasks = len(req['map_tasks'])
                num_reduce_tasks = len(req['reduce_tasks'])
                total_jobs = num_map_tasks + num_reduce_tasks

                mapTaskCounts[req['job_id']] = num_map_tasks
                jobTaskCounts[req['job_id']] = total_jobs
                reduceTasks[req['job_id']] = list()
                tasks = req['reduce_tasks']
                task = dict()

                for i in tasks:
                    task[i['task_id']] = dict()
                    task[i['task_id']]['task_id'] = i['task_id'] 
                    task[i['task_id']]['job_id'] = req['job_id']
                    task[i['task_id']]['duration'] = i['duration']
                    reduceTasks[req['job_id']].append(task[i['task_id']])
                

                tasks = req['map_tasks']
                task = dict()
                for i in tasks:
                    task[i['task_id']] = dict()
                    task[i['task_id']]['task_id'] = i['task_id'] 
                    task[i['task_id']]['job_id'] = req['job_id']
                    task[i['task_id']]['duration'] = i['duration']
                    taskQueue.append(task[i['task_id']])
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
            else:
                c.close()
                        
    
    
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
    else:
        s.close()


#Get worker updates                
def UpdateWorkers():    
    host = ""
    port = 5001
    
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        s.bind((host, port))
        s.listen(1000)
    
        for i in iter(int,1):
            try:
                c, addr = s.accept()
                
                f = open("log/master.txt", "a+")
                try:
                    printLock.acquire()
                    print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tCONNECTION INCOMING = [host:{addr[0]}, port:{addr[1]}]",file=f)
                    printLock.release()
                except:
                    print("UNKNOWN ERROR OCCURED DURING LOGGING")
                else:
                    f.close()
                
                with c:
                    update_json = c.recv(1000)
                    update = json.loads(update_json)
                    worker_id = update['worker_id']
                    job_id = update['job_id']
                    task_id = update['task_id']


                    f = open("log/master.txt", "a+")
                    try:
                        printLock.acquire()
                        print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\t TASK COMPLETED AT = [worker_id:{worker_id}, task_id:{task_id}]",file=f)
                        printLock.release()
                    except:
                        print("UNKNOWN ERROR OCCURED DURING LOGGING")    
                    else:
                        f.close()
                    
                    print(f"Task {task_id} completed execution in {worker_id}")
                    jobTaskCounts[job_id] = jobTaskCounts[job_id] - 1
                    
                    if jobTaskCounts[job_id] != 0:
                        pass
                    
                    else:
                        f = open("log/master.txt", "a+")
                        try:
                            printLock.acquire()
                            print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tFINISHED EXECUTING JOB = [job_id:{job_id}]",file=f)
                            printLock.release()
                        except:
                            print("UNKNOWN ERROR OCCURED DURING LOGGING")
                        else:
                            f.close()    

                #prevent race condition 
                    workerLock.acquire()
                    availableSlots[worker_id]['slots'] = availableSlots[worker_id]['slots'] + 1
                    workerLock.release()
                    
                    if 'M' not in task_id:
                        pass
                    else:
                        #map job is completed
                        mapTaskCounts[job_id] = mapTaskCounts[job_id] - 1
                        if mapTaskCounts[job_id] != 0:
                            pass
                        else:
                            for reduce_task in reduceTasks[job_id]:
                                taskQueue.append(reduce_task)
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
            else:
                c.close() 

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
    else:
        s.close()



#allocate tasks to the right worker     
def Schedule(): 
    functions = {"RANDOM":RANDOM,"RR":RR,"LL":LL}
    scheduling_algo = sys.argv[2]
    for i in iter(int,1):
        taskQueueSize = len(taskQueue)
        if(taskQueueSize < 0):
            #There are no tasks in the queue 
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
                task = taskQueue.pop(0)
                task_message = json.dumps(task)
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect(("localhost", port))
                    f = open("log/master.txt", "a+")
            
                    try:
                        printLock.acquire()
                        print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tCONNECTION OUTGOING = [host:{'localhost'}, port:{port}]",file=f)
                        printLock.release()
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
                else: 
                    s.close()
                f = open("log/master.txt", "a+")
                try:
                    printLock.acquire()
                    print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f"\tTASK SENT TO = [worker_id:{worker_id}, task_id:{task['task_id']}, job_id:{task['job_id']}, duration:{task['duration']}]",file=f)
                    printLock.release()
                except:
                    print("UNKNOWN ERROR OCCURED DURING LOGGING")
                else:
                    f.close()

                print(f"Sent task {task['task_id']} to worker {worker_id} on port {port}")
                
                workerLock.acquire()
                availableSlots[worker_id]['slots'] = availableSlots[worker_id]['slots'] - 1
                workerLock.release()
    
                                 
if not(os.path.isdir('log')):
    os.mkdir('log')

open("log/master.txt", "w").close()
f = open(sys.argv[1])
try:
    config = json.load(f)
except:
        print("UNKNOWN ERROR OCCURED DURING LOGGING")
else:
    f.close()

availableSlots = dict() 
for worker in config['workers']:
    slots = worker['slots']
    port = worker['port']
    availableSlots[worker['worker_id']] = {'slots':slots, 'port':port}

print(availableSlots)
thread1 = myThread(1, Requests)
thread2 = myThread(2, UpdateWorkers)
thread3 = myThread(3, Schedule)

#Start thread execution
thread1.start()
thread2.start()
thread3.start()

#Join threads
thread1.join()
thread2.join()
thread3.join()
