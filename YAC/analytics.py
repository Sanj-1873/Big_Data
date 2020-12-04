import re
import datetime
import statistics

task_regex = re.compile("(\d{2}\/\d{2}\/\d{4}, \d{2}:\d{2}:\d{2}.\d{6}):\sTASK\s(RECEIVED|FINISHED|STARTED) = \[job_id:(\d+), task_id:(\d+_[MR]\d+)(, duration:\d+)?]")
jobs_received = re.compile("(\d{2}\/\d{2}\/\d{4}, \d{2}:\d{2}:\d{2}.\d{6})\sRECEIVED JOB REQUEST = \[job_id:(\d+).*")
last_reduce = re.compile("'(\d+_R\d+)']]$")

jobs=dict()
task_times = dict()
job_times= dict()

for w_id in range(1,4):
    with  open(f"log/worker_{w_id}.txt") as worker:
        tasks=dict()
        for line in worker:
            try:
                tobj = task_regex.match(line)
                if tobj.group(2)=="FINISHED":
                    t_stamp = datetime.datetime.strptime(tobj.group(1),'%d/%m/%Y, %H:%M:%S.%f')
                    tasks[tobj.group(4)]['finished']=t_stamp
                elif tobj.group(2)=="RECEIVED":
                    t_stamp = datetime.datetime.strptime(tobj.group(1),'%d/%m/%Y, %H:%M:%S.%f')
                    tasks[tobj.group(4)]={"received":t_stamp}
            except AttributeError as e:
                continue
    task_times[w_id]=[(tasks[x]['finished']-tasks[x]['received']).total_seconds() for x in tasks]


with open("log/master.txt") as master:
    for line in master:
        try:
            jobj = jobs_received.match(line)
            t_stamp = datetime.datetime.strptime(jobj.group(1),'%d/%m/%Y, %H:%M:%S.%f')
            jobs[int(jobj.group(2))] = {"time":t_stamp,"last_reduce":last_reduce.findall(jobj.group(0))[0]}
        except AttributeError as e:
            continue

for w_id in range(1,4):
    with open(f"log/worker_{w_id}.txt") as worker:
        for line in worker:
            try:
                tobj = task_regex.match(line)
                if tobj.group(2)=='FINISHED':
                    for job in jobs:
                        if jobs[job]['last_reduce']==tobj.group(4):
                            t_stamp = datetime.datetime.strptime(tobj.group(1),'%d/%m/%Y, %H:%M:%S.%f')
                            job_times[job] = (t_stamp - jobs[job]['time']).total_seconds()
            except AttributeError as e:
                continue
job_means = statistics.mean(job_times.values())
job_medians = statistics.median(job_times.values())
task_means = {x:statistics.mean(task_times[x]) if len(task_times[x]) else 0 for x in task_times}
task_medians = {x:statistics.median(task_times[x]) if len(task_times[x]) else 0 for x in task_times}



print(task_means,task_medians,job_means,job_medians,sep="\n")