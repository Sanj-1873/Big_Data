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
        self.free_slots = 0
        try:
            self.slots = slots[self.slot_count]
        except:
            self.slots = 0
        self.free_slots = self.slots
        self.exec_pool = [0 for x in range(self.slots)]
        
        try: 
            f = open(f"log/worker_{w_id}.txt", "a+")
            log_lock.acquire()
            print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f":\tWORKER HAS STARTED = [worker_id:{self.worker_id}, port:{self.port}, slots:{self.slots}]",file=f)
            log_lock.release()
        except:
            print("UNKNOWN ERROR OCCURED DURING LOGGING")
        else:
            f.close()

    def task_launch(self, task_dict):
        task = Task(task_dict["job_id"], task_dict["task_id"], task_dict["duration"])
        try :
            f = open(f"log/worker_{w_id}.txt", "a+")
            log_lock.acquire()
            print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f":\tTASK STARTED = [job_id:{task_dict['job_id']}, task_id:{task_dict['task_id']}]",file=f)
            log_lock.release()
        except:
            print("UNKNOWN ERROR OCCURED DURING LOGGING")
        else:
            f.close()
        
        limit = self.slots
        for i in range(limit):
            if not(type(self.exec_pool[i])==type(1) and self.exec_pool[i] == 0):
                pass
            else:
                self.exec_pool[i] = task
                self.free_slots = self.free_slots - 1
                break
            
    def remove_task(self, exec_pool_index):
        self.exec_pool[exec_pool_index] = 0
        self.free_slots = self.free_slots + 1

#Task def
class Task:
    def __init__(self, job_id, task_id, time_left):
        self.time_left = time_left
        self.task_id = task_id
        self.job_id = job_id

    def reduce(self):
        self.time_left = self.time_left - 1



def get_task_launch_msg(worker):
    try:
        s = socket(AF_INET, SOCK_STREAM)
        s.bind(("localhost", worker.port))
        s.listen(1024)
        for i in iter(int,1):
            c, addr = s.accept()
            try:
                f = open(f"log/worker_{w_id}.txt", "a+")
                log_lock.acquire()
                print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f":\tCONNECTION INCOMING = [host:{addr[0]}, port:{addr[1]}]",file=f)
                log_lock.release()
            except:
                print("UNKNOWN ERROR OCCURED DURING LOGGING")
            else:
                f.close()
            with c:
                task_launch_msg_json = c.recv(1024).decode()
                if not task_launch_msg_json:
                    continue
                else:
                    task = json.loads(task_launch_msg_json)
                    try:
                        f = open(f"log/worker_{w_id}.txt", "a+")
                        log_lock.acquire()
                        print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f":\tTASK RECEIVED = [job_id:{task['job_id']}, task_id:{task['task_id']}, duration:{task['duration']}]",file=f)
                        log_lock.release()
                    except:
                        print("UNKNOWN ERROR OCCURED DURING LOGGING")
                    else:
                        f.close()
                    workers_lock.acquire()
                    worker.task_launch(task)
                    workers_lock.release()
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

#4th thread goes through each task and reduce the time for each task       
def execution_of_tasks(worker):
    for i in iter(int,1):
        if(not(worker.slots)):
        	continue    
        for i in range(worker.slots):
            if (type(worker.exec_pool[i])==type(1)) and not(worker.exec_pool[i]):
                continue
            elif (worker.exec_pool[i].time_left>0):
                task = worker.exec_pool[i]
                job_id = task.job_id
                task_id = task.task_id
                msg = {"worker_id": worker.worker_id,"job_id": job_id, "task_id": task_id}
                try:
                    f =  open(f"log/worker_{w_id}.txt", "a+")
                    log_lock.acquire()
                    print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f":\tTASK FINISHED = [job_id:{job_id}, task_id:{task_id}]",file=f)
                    log_lock.release()
                except:
                    print("UNKNOWN ERROR OCCURED DURING LOGGING")
                else:
                    f.close()
                workers_lock.acquire()
                worker.remove_task(i)
                workers_lock.release()
                
                msg_str = json.dumps(msg)
                s = socket(AF_INET, SOCK_STREAM)
                try:
                    s.connect(('localhost', 5001))
                    f = open(f"log/worker_{w_id}.txt", "a+")
                    try:
                        log_lock.acquire()
                        print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f":\tCONNECTION OUTGOING = [host:{'localhost'}, port:{5001}]",file=f)
                        log_lock.release()
                    except:
                        print("UNKNOWN ERROR OCCURED DURING LOGGING")
                    else:
                        f.close()
                    s.send(msg_str.encode())
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
                    f = open(f"log/worker_{w_id}.txt", "a+")
                    log_lock.acquire()
                    print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f") + f":\tUPDATE SENT TO MASTER = [job_id:{job_id}, task_id:{task_id}] COMPLETED",file=f)
                    log_lock.release()
                except:
                    print("UNKNOWN ERROR OCCURED DURING LOGGING")
                else:
                    f.close()
            else:
                workers_lock.acquire()
                worker.exec_pool[i].reduce()
                workers_lock.release()
    sleep(1)
  
tasks_lock = threading.Lock()
workers_lock = threading.Lock()
log_lock = threading.Lock() 


try:
    os.mkdir('log')
except:
    pass
open(f"log/worker_{w_id}.txt", "w").close()
if (len(sys.argv) != 3):
    print("Usage: python Worker.py <port number> <worker id>")

worker = Worker(int(sys.argv[1]),int(sys.argv[2]))
t1 = threading.Thread(target = get_task_launch_msg, args = (worker,))
t2 = threading.Thread(target = execution_of_tasks, args = (worker,))
t1.start()
t2.start()
t1.join()
t2.join()