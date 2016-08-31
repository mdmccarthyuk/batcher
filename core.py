#!/usr/bin/python

import sys, time, argparse, sqlite3, os
from subprocess import Popen, PIPE
from daemon import Daemon
from task import TaskRunner
from host import Host

class BatcherDaemon(Daemon):
  def run(self):
    print "STARTING"
    main(None)

def main(args):
  global checkRunning,checkResult,debugFlag,hostList,runningTasks

  runningTasks = dict()
  taskCount = 0
  taskMax = 3
  hostList = dict()
  checkRunning = dict()
  checkResult = dict()

  print "Making FIFO"
  if os.path.exists('/var/run/batcher/batcher'):
    os.remove('/var/run/batcher/batcher')

  os.mkfifo('/var/run/batcher/batcher')
  pipeIn = os.open('/var/run/batcher/batcher', os.O_RDONLY|os.O_NONBLOCK)
  pipeRead = ""

  while pipeRead != "QUIT":
    print "main> Heartbeat"

    worker_getHosts()
    worker_getTasks()

    for host in hostList:
      print "HOST: %s = %s (%s)" % (host, hostList[host].name, hostList[host].method)
      host_checkLoad(hostList[host])

    completeTasks=[]
    for task in runningTasks:
      if runningTasks[task].state == 'INIT':
        if taskCount < taskMax:
          taskCount += 1
          runningTasks[task].start()
        else:
          print "main> waiting for worker: %s" % task
      else:
        if runningTasks[task].state in ["COMPLETING","COMPLETE"]:
          completeTasks.append(task)
          taskCount -= 1
          task_done(task)

    for task in completeTasks:
      del runningTasks[task]

    time.sleep(5)
    pipeRead = os.read(pipeIn,1024).strip()

  print "Exiting";
  os.close(pipeIn)
  os.remove('/var/run/batcher/batcher')

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
  row = c.fetchone()
  while row is not None:
    if row[1] not in hostList:
      newHost = Host(row[1])
      if row[2] == 'ssh':
        newHost.sshAccess(row[3])
      hostList[row[1]]=newHost
    row = c.fetchone()
  conn.close()

def worker_getTasks():
  global taskList,runningTasks
  conn = sqlite3.connect('/var/run/batcher/core.db')
  c = conn.cursor()
  sql = 'SELECT id,status,task,time,host,pid FROM tasks WHERE NOT status=\'DONE\''
  c.execute(sql)
  row = c.fetchone()
  while row is not None:
    if row[0] not in runningTasks:
      newTask=TaskRunner(row[2],hostList[row[4]])
      runningTasks[row[0]]=newTask
    row = c.fetchone()
  conn.close()

def cmd_host(args):
  if args.list == True:
    print "HOST LIST"
    host_list()
    sys.exit(0)
  if args.add != None:
    print "HOST ADD"
    host_add(args.add,args.method,args.user)
    sys.exit(0)
  if args.delete != None:
    print "HOST DELETE"
    host_delete(args.delete)
    sys.exit(0)

def cmd_service(args):
  daemon = BatcherDaemon('/var/run/batcher/batcher.pid')
  print "cmd_service> entered"
  if args.action == 'start':
    print "cmd_service> daemon starting"
    daemon.start()
    print "cmd_service> daemon started"
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

def host_add(hostname,method,user):
  if method == 'ssh':
    accessType='ssh'
    accessUser=user
  elif method == 'local':
    accessType='local'
    accessUser='local'
  else:
    print "host_add> invalid access Type - %s" % method
    sys.exit(1)
  conn = sqlite3.connect('/var/run/batcher/core.db')
  c = conn.cursor()
  c.execute('INSERT INTO HOSTS (hostname, access, user) values (?, ?, ?)', (hostname, accessType,accessUser))
  conn.commit()
  conn.close()

def host_checkLoad(host):
  global checkRunning,checkResult
  if host.method == "ssh":
    command = "ssh %s@%s 'cat /proc/loadavg /proc/stat'" % (host.user,host.name)
  elif host.method == "local":
    command = "cat /proc/loadavg /proc/stat"
  else:
    print "host_checkLoad> Unknown access method"
    sys.exit(1)
  
  if host.name not in checkRunning:
    print "host_checkLoad> %s check triggered" % host.name
    checkRunning[host.name]=Popen(command,stdout=PIPE,stderr=PIPE,shell=True)
  else:
    taskproc = checkRunning[host.name]
    retcode = taskproc.poll()
    if retcode is not None:
      checkResult[host.name] = taskproc.stdout.read()
      del checkRunning[host.name]
      lines = checkResult[host.name].split('\n')
      loads = lines[0].split()
      cpustat = lines[1].split()
      cpuTotal = eval("%s+%s+%s+%s+%s+%s+%s" % (cpustat[1],cpustat[2],cpustat[3],cpustat[4],cpustat[5],cpustat[6],cpustat[7]))
      host.loads['iowait'] = float(cpustat[5])/cpuTotal
      host.loads['load'] = float(loads[0])
      print "host_checkLoad> %s load is %s" % (host.name,host.loads['load'])
      print "host_checkLoad> %s IOWait is %s" % (host.name,host.loads['iowait'])
      if float(host.loads['load']) > 0.10:
        print "host_checkLoad> %s load is over threshold" % host.name
    else:
      print "host_checkLoad> %s check still running" % host.name

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
  parser_daemon.add_argument('-d', '--debug',action='store_true')
  parser_daemon.set_defaults(func=cmd_service) 
 
  parser_host = subparsers.add_parser('host')
  parser_host.add_argument('-l', '--list',action='store_true')
  parser_host.add_argument('-a', '--add')
  parser_host.add_argument('-m', '--method',default='ssh')
  parser_host.add_argument('-u', '--user')
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

