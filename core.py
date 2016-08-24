#!/usr/bin/python

import sys, time, argparse, sqlite3, os
from subprocess import Popen, PIPE
from daemon import Daemon

class BatcherDaemon(Daemon):
  def run(self):
    print "STARTING"
    main(None)

def main(args):
  global taskList,taskStatus,checkRunning,checkResult,debugFlag,hostAccess,taskRunning

  if args.debug is True:
    debugFlag = True
    print "DEBUG"

  taskCount = 0
  taskMax = 1
  taskList = dict()
  taskStatus = dict()
  taskRunning = dict()
  hostList = dict()
  hostAccess = dict()
  checkRunning = dict()
  checkResult = dict()

  while True:
    print "main> Heartbeat"

    hosts = worker_getHosts()
    for host in hosts:
      hostAccess[host[1]] = [ host[2], host[3] ]
      host_checkLoad(host[1])

    tasks = worker_getTasks()
    for task in tasks:
      print "main> task: %s %s - %s" % (task[0],taskStatus[task[0]],task[2])
      if taskStatus[task[0]] == 'init':
        if taskCount < taskMax:
          taskCount += 1
          task_init(task[0],task[2],task[4])
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
            taskCount -= 1
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

def worker_getHosts():
  global hostList
  conn = sqlite3.connect('/var/run/batcher/core.db')
  sql = 'select * from hosts'
  c = conn.cursor()
  c.execute(sql)
  hosts = []
  row = c.fetchone()
  while row is not None:
    hosts.append(row)
    row = c.fetchone()
  conn.close()
  return hosts

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

def task_init(task,cmd,host):
  global taskStatus,taskRunning,hostAccess
  if hostAccess[host][0] == "ssh":
    command = "ssh %s@%s '%s'" % (hostAccess[host][1],host,cmd)
  elif hostAccess[host][0] == "local":
    command = cmd  
  else:
    print "task_init> hostAccess method invalid"
    sys.exit(1)
  print "task_init> task %s - %s starting" % (task, cmd)
  taskRunning[task] = Popen(command,shell=True,stdout=PIPE,stderr=PIPE)
  taskStatus[task] = "STARTED"
  print "task_init> task %s - %s started" % (task, cmd)

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
  conn.close()

def host_checkLoad(hostname):
  global checkRunning,checkResult,hostAccess
  if hostAccess[hostname][0] == "ssh":
    command = "ssh %s@%s 'cat /proc/loadavg /proc/stat'" % (hostAccess[hostname][1],hostname)
    print "host_checkLoad> ssh method"
  elif hostAccess[hostname][0] == "local":
    command = "cat /proc/loadavg /proc/stat"
    print "host_checkLoad> local method"
  else:
    print "host_checkLoad> Unknown access method"
    sys.exit(1)
  
  if hostname not in checkRunning:
    print "host_checkLoad> %s check triggered" % hostname
    checkRunning[hostname]=Popen(command,stdout=PIPE,stderr=PIPE,shell=True)
  else:
    taskproc = checkRunning[hostname]
    retcode = taskproc.poll()
    if retcode is not None:
      checkResult[hostname] = taskproc.stdout.read()
      del checkRunning[hostname]
      lines = checkResult[hostname].split('\n')
      loads = lines[0].split()
      cpustat = lines[1].split()
      cpuTotal = eval("%s+%s+%s+%s+%s+%s+%s" % (cpustat[1],cpustat[2],cpustat[3],cpustat[4],cpustat[5],cpustat[6],cpustat[7]))
      cpuWait = float(cpustat[5])/cpuTotal 
      print "host_checkLoad> %s load is %s" % (hostname,loads[0])
      print "host_checkLoad> %s IOWait is %s" % (hostname,cpuWait)
      if float(loads[0]) > 0.10:
        print "host_checkLoad> %s load is over threshold" % hostname
    else:
      print "host_checkLoad> %s check still running" % hostname

if __name__ == "__main__":
  workerStatus="START"
  debugFlag = False
  check_for_db()
  parser = argparse.ArgumentParser(description="Batcher core")
  subparsers = parser.add_subparsers()

  parser_standalone = subparsers.add_parser('standalone')
  parser_standalone.add_argument('-d', '--debug',action='store_true')
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

