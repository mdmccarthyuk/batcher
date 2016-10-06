#!/usr/bin/python

import os, signal, threading, time, sys
from subprocess import Popen, PIPE
from host import Host

class TaskRunner (threading.Thread):

  nextTaskID = 0
  lastPriority = 100
  lastChangeTick = 0

  def __init__(self,cmd,host):
    self.state="INIT"
    TaskRunner.nextTaskID+=1
    self.ID=TaskRunner.nextTaskID
    self.killable = False
    self.loaded = False
    self.host = host
    self.monitorHosts = []
    self.cmd = cmd
    self.priority = 100
    threading.Thread.__init__(self)

  def run(self):
    print "Starting thread on host - %s" % self.host.name 
    print "Monitoring on"
    for host in self.monitorHosts:
      print host.name
    self.runTask()
    print "Exiting thread"

  def addMonitorHost(self,host):
    self.monitorHosts.append(host)

  def runTask(self):
    while self.state != "COMPLETE":
      self.transition()
      print "THREAD %s HEARTBEAT - %s" % (self.ID,self.state)
      self.loaded = False
      for host in self.monitorHosts:
        for load in self.host.loads:
          if self.host.loads[load] > self.host.limits[load]:
            self.loaded = True
            print "Task running on loaded host"

        if self.state == "PAUSED" and self.loaded == False:
          self.resumeTask()
        if self.state == "RUNNING" and self.loaded == True:
          self.pauseTask()

      time.sleep(1)

  def startTask(self):
    if self.host.method == 'ssh':
      command = "ssh %s@%s '%s'" % ( self.host.user, self.host.name, self.cmd)
    elif self.host.method == 'local':
      command = self.cmd
    else:
      print "task method invalid"
      sys.exit(1)
    self.process = Popen(command,shell=True,stdout=PIPE,stderr=PIPE)
    print "PID: %s" % self.process.pid

  def transition(self):
    if self.state == "INIT":
      self.startTask()
      self.state = "STARTING"

    if self.state == "STARTING":
      self.state = "RUNNING"

    if self.state == "COMPLETING":
      self.state = "COMPLETE"

    if self.state == "RUNNING":
      taskproc = self.process
      retcode = taskproc.poll()
      if retcode is not None:
        self.completeTask()
        print "------STDOUT [%s]------" % self.ID
        print taskproc.stdout.read()
        print "------STDERR [%s]------" % self.ID
        print taskproc.stderr.read()
        print "-----------------------"

    if self.state == "PAUSING":
      self.state = "PAUSED"

    if self.state == "RESUMING":
      self.state = "RUNNING"

  def pauseTask(self):
    self.state="PAUSING"
    if self.host.method == "local":
      if self.killable:
        os.kill(self.process.pid, signal.SIGTERM)
      else:
        os.kill(self.process.pid, signal.SIGSTOP)
    elif self.host.method == "ssh":
      print "ssh: pause IOU"
    else:
      print "Invalid host method"

  def resumeTask(self):
    self.state="RESUMING"
    if self.host.method == "local":
      if self.killable:
        self.startTask()
      else:
        os.kill(self.process.pid, signal.SIGCONT)
    elif self.host.method == "ssh":
      print "ssh: resume IOU"
    else:
      print "Invalid host method"

  def killTask(self):
    self.killable=True
    self.pauseTask()

  def stopTask(self):
    self.state="STOPPING"

  def completeTask(self):
    self.state="COMPLETING"

if __name__ == "__main__":

  sys.exit(0)
