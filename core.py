#!/usr/bin/python

import sys
import time
import argparse
import sqlite3
import os
from subprocess import Popen, PIPE
from daemon import Daemon

class BatcherDaemon(Daemon):
  def run(self):
    print "STARTING"
    main(None)

def main(args):
  global taskList,taskStatus

  taskCount = 0
  taskMax = 1
  taskList = dict()
  taskStatus = dict()
  taskRunning = dict()

  while True:
    print "main> Heartbeat"
    tasks = worker_getTasks()
    for task in tasks:
      print "main> task: %s %s - %s" % (task[0],taskStatus[task[0]],task[2])
      if taskStatus[task[0]] == 'init':
        if taskCount < taskMax:
          taskCount = taskCount+1
          taskStatus[task[0]]='STARTED'
          print "main> init: %s" % task[0]
          taskRunning[task[0]] = Popen([task[2]],stdout=PIPE,stderr=PIPE)
        else:
          print "main> waiting for worker: %s" % task[0]
      else:
        if taskStatus[task[0]] == 'STARTED':
          taskproc = taskRunning[task[0]]
          retcode = taskproc.poll()
          if retcode is not None:
            print "main> task done %s (%s) - return: %s" % (task[0], task[2], retcode)
            del taskRunning[task[0]]
            taskStatus[task[0]]='DONE'
            print "main> ----- STDOUT -----"
            print taskproc.stdout.read()
            print "main> ----- STDERR -----"
            print taskproc.stderr.read()
            print "main> ------------------"
            taskCount = taskCount - 1
            task_done(task[0])
           
    time.sleep(5)    

def check_for_db():
  conn = sqlite3.connect('/var/run/batcher/core.db')
  sql = 'create table if not exists hosts (id INTEGER PRIMARY KEY, hostname text, access text, user text)'
  c = conn.cursor()
  c.execute(sql)
  conn.commit()
  sql = 'create table if not exists tasks (id INTEGER PRIMARY KEY, status text, task text, time text, host text, pid INTEGER)'
  c.execute(sql)
  conn.commit() 
  conn.close()

def worker_getTasks():
  global taskList,taskStatus
  conn = sqlite3.connect('/var/run/batcher/core.db')
  c = conn.cursor()
  sql = 'SELECT id,status,task,time,host,pid FROM tasks WHERE NOT status=\'DONE\''
  c.execute(sql)
  tasks = []
  row = c.fetchone()
  while row is not None:
    if row[0] not in taskList: 
      taskList[row[0]] = row[2]
      taskStatus[row[0]] = row[1]
    tasks.append(row)
    row = c.fetchone()
  conn.close()
  return tasks

def cmd_host(args):
  if args.list == True:
    print "HOST LIST"
    host_list()
    sys.exit(0)
  if args.add != None:
    print "HOST ADD"
    host_add(args.add)
    sys.exit(0)
  if args.delete != None:
    print "HOST DELETE"
    host_delete(args.delete)
    sys.exit(0)

def cmd_service(args):
  daemon = BatcherDaemon('/var/run/batcher/batcher.pid')
  if args.action == 'start':
    daemon.start()
    sys.exit(0)
  elif args.action == 'stop':
    daemon.stop()
    sys.exit(0)
  elif args.action == 'restart':
    daemon.restart()
    sys.exit(0)
  else:
    print "Invalid service command\n";
    sys.exit(1)

def cmd_task(args):
  if args.list == True:
    task_list()
    sys.exit(0)
  if args.add != None:
    if args.host == 'null':
      print "Host for task not specified"
      sys.exit(1)
    task_add(args.add,args.host)
    sys.exit(0)

def cmd_worker(args):
  global workerStatus
  if args.status == True:
    if workerStatus == "STOP":
      print "Worker is stopped"
    elif workerStatus == "START":
      print "Worker is started"
    else:
      print "Worker is in state %s" % workerStatus
  if args.go == True:
    workerStatus="START"
  if args.stop == True:
    workerStatus="STOP"
  sys.exit(0)

def task_list():
  conn = sqlite3.connect('/var/run/batcher/core.db')
  sql = 'select * from tasks'
  c = conn.cursor()
  c.execute(sql)
  row = c.fetchone()
  while row is not None:
    print row
    row = c.fetchone()
  conn.close()

def task_add(task,host):
  conn = sqlite3.connect('/var/run/batcher/core.db')
  c = conn.cursor()
  c.execute('INSERT INTO tasks (status, task, host) values (\'init\', ?, ?)', (task, host,))
  conn.commit() 
  conn.close()  

def task_done(task):
  conn = sqlite3.connect('/var/run/batcher/core.db')
  c = conn.cursor()
  c.execute('UPDATE tasks SET status=\'DONE\' WHERE id = ?', (task,))
  conn.commit()
  conn.close()

def host_list():
  conn = sqlite3.connect('/var/run/batcher/core.db')
  sql = 'select hostname from hosts'
  c = conn.cursor()
  c.execute(sql)
  row = c.fetchone()
  while row is not None:
    print row[0]
    row = c.fetchone()
  conn.close()

def host_add(hostname):
  conn = sqlite3.connect('/var/run/batcher/core.db')
  c = conn.cursor()
  c.execute('INSERT into hosts (hostname, access, user) values (?, \'ssh\', \'mikmcc\')', (hostname,))
  conn.commit()
  conn.close()

if __name__ == "__main__":
  workerStatus="START"
  check_for_db()
  parser = argparse.ArgumentParser(description="Batcher core")
  subparsers = parser.add_subparsers()

  parser_standalone = subparsers.add_parser('standalone')
  parser_standalone.set_defaults(func=main)

  parser_daemon = subparsers.add_parser('service')
  parser_daemon.add_argument('-k', '--action')
  parser_daemon.set_defaults(func=cmd_service) 
 
  parser_host = subparsers.add_parser('host')
  parser_host.add_argument('-l', '--list',action='store_true')
  parser_host.add_argument('-a', '--add')
  parser_host.add_argument('-d', '--delete')
  parser_host.set_defaults(func=cmd_host)

  parser_task = subparsers.add_parser('task')
  parser_task.add_argument('-l','--list',action='store_true')
  parser_task.add_argument('-H','--host',default="null")
  parser_task.add_argument('-a','--add')
  parser_task.set_defaults(func=cmd_task)

  parser_worker = subparsers.add_parser('worker')
  parser_worker.add_argument('-g','--go',action='store_true')
  parser_worker.add_argument('-s','--stop',action='store_true')
  parser_worker.add_argument('--status',action='store_true')
  parser_worker.set_defaults(func=cmd_worker)

  args = parser.parse_args()
  args.func(args)

