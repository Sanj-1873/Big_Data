from datetime import datetime
import json
import os
import random
from socket import *
import sys
import threading
import time
from time import *


w_id = sys.argv[2]
slots=[5,7,3]


#Worker definition
class Worker:
    #set the initial values 
    def __init__(self, port, worker_id):
        self.port = port
        self.worker_id = worker_id
        self.exec_pool = list()
        self.free_slots = 0
        
        self.slot_count = worker_id-1
        self.slots = slots[self.slot_count]
        self.free_slots = slots[self.slot_count]
        self.exec_pool = [0 for x in range(slots[self.slot_count])]
        
        try: 
            f = open(f"log/worker_{w_id}.txt", "a+")
            printLock.acquire()
            print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f":\tWORKER HAS STARTED = [worker_id:{self.worker_id}, port:{self.port}, slots:{self.slots}]",file=f)
            printLock.release()
        finally:
            f.close()

    
    #get the task message and lauch the task if the there is a free slot
    def task_launch(self, task_dict):
        task = Task(task_dict["job_id"], task_dict["task_id"], task_dict["duration"])
        
        try :
            f = open(f"log/worker_{w_id}.txt", "a+")
            printLock.acquire()
            print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f":\tTASK STARTED = [job_id:{task_dict['job_id']}, task_id:{task_dict['task_id']}]",file=f)
            printLock.release()
        finally:
            f.close()
        
        limit = len(self.exec_pool)
        for i in range(limit):
            if (not isinstance(self.exec_pool[i], int) or self.exec_pool[i] != 0):
                pass
            else:
                self.exec_pool[i] = task
                self.free_slots = self.free_slots - 1
                break
            
    
   # When time is 0, remove the task form the exectuion pool 
    def remove_task(self, exec_pool_index):
        self.exec_pool[exec_pool_index] = 0
        self.free_slots = self.free_slots + 1

#Task def
class Task:
    #set the values up for the job, task and time remaining
    def __init__(self, job_id, task_id, time_left):
        self.time_left = time_left
        self.task_id = task_id
        self.job_id = job_id
       
    
    #decrement by 1 
    def reduce(self):
        self.time_left = self.time_left - 1
        
    #is there time left in the task 
    def check_time_remaining(self):
        time = self.time_left
        return bool(time)




#1st thread; listen for the "task launch" message and then adds the task to the execution pool
def get_task_launch_msg(worker):
    s = socket(AF_INET, SOCK_STREAM)
    with s:
        s.bind(("localhost", worker.port))
        s.listen(1024)
        for i in iter(int,1):
            c, addr = s.accept()
            
            try:
                f = open(f"log/worker_{w_id}.txt", "a+")
                printLock.acquire()
                print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f":\tCONNECTION INCOMING = [host:{addr[0]}, port:{addr[1]}]",file=f)
                printLock.release()
            finally:
                f.close()
            with c:
                task_launch_msg_json = c.recv(1024).decode()
                if not task_launch_msg_json:
                    pass
                else:
                    task = json.loads(task_launch_msg_json)
                    
                    try:
                        f = open(f"log/worker_{w_id}.txt", "a+")
                        printLock.acquire()
                        print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f":\tTASK RECEIVED = [job_id:{task['job_id']}, task_id:{task['task_id']}, duration:{task['duration']}]",file=f)
                        printLock.release()
                    finally:
                        f.close()
                    workerLock.acquire()
                    worker.task_launch(task)
                    workerLock.release()
        
        
#4th thread goes through each task and reduce the time for each task       
def execution_of_tasks(worker):
    for i in iter(int,1):
        #if there are workers 
        if(worker.slots == 0):
        	continue    
        for i in range(worker.slots):
            if (isinstance(worker.exec_pool[i], int) and worker.exec_pool[i] == 0):
                continue
            elif (worker.exec_pool[i].check_time_remaining()):
                task = worker.exec_pool[i]
                job_id = task.job_id
                task_id = task.task_id
                msg = { 
                    "worker_id": worker.worker_id, 
                    "job_id": job_id, 
                    "task_id": task_id
                    }
                    
                try:
                    f =  open(f"log/worker_{w_id}.txt", "a+")
                    printLock.acquire()
                    print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f":\tTASK FINISHED = [job_id:{job_id}, task_id:{task_id}]",file=f)
                    printLock.release()
                finally:
                    f.close()

                workerLock.acquire()
                worker.remove_task(i)
                workerLock.release()
                
                msg_str = json.dumps(msg)
                s = socket(AF_INET, SOCK_STREAM)
                with s:
                    s.connect(('localhost', 5001))
                    f = open(f"log/worker_{w_id}.txt", "a+")
                    #with open(f"log/worker_{w_id}.txt", "a+") as f:
                    try:
                        printLock.acquire()
                        print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f":\tCONNECTION OUTGOING = [host:{'localhost'}, port:{5001}]",file=f)
                        printLock.release()
                    finally:
                        f.close()
                    s.send(msg_str.encode())
                #with open(f"log/worker_{w_id}.txt", "a+") as f:
                try:
                    f = open(f"log/worker_{w_id}.txt", "a+")
                    printLock.acquire()
                    print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f":\tUPDATE SENT TO MASTER = [job_id:{job_id}, task_id:{task_id}] COMPLETED",file=f)
                    printLock.release()
                finally:
                    f.close()
            else:
                workerLock.acquire()
                worker.exec_pool[i].reduce()
                workerLock.release()
    sleep(1)
  
taskLock = threading.Lock()
workerLock = threading.Lock()
printLock = threading.Lock() 


try:
    os.mkdir('log')
except:
    pass
open(f"log/worker_{w_id}.txt", "w").close()
if (len(sys.argv) != 3):
    print("Usage: python Worker.py <port number> <worker id>")
worker = Worker(int(sys.argv[1],int(sys.argv[2]))

t1 = threading.Thread(target = get_task_launch_msg, args = (worker,))
t2 = threading.Thread(target = execution_of_tasks, args = (worker,))
t1.start()
t2.start()
t1.join()
t2.join()