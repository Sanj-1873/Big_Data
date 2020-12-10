# Big_Data
This project is meant to simulate the master worker scheduling on your local machine

The Master process has 2 threads - one to listen for job requests, and another to listen for updates from Workers . Each Worker has 2 threads-one for listening for task launch messages from Master, and another to simulate the execution of the tasks and to send updates to the Master.

To execute, run the following commands on different terminals 

```bash
python3 worker.py 4002 3
python3 worker.py 4001 2
python3 worker.py 4000 1
python3 master.py config.json RR 
python3 requests.py 5
```

This specific set of commands used Round Robin, RR can be changed to LL or Random to use a different algo. 

