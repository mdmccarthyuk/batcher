#!/usr/bin/python

import os
import signal
import threading
import time
import sys
import syslog
from subprocess import Popen, PIPE
from host import Host
from Queue import Queue,Empty
from stream import StreamReader

class TaskRunner (threading.Thread):

  syslog.openlog('batcher')

  nextTaskID = 0
  lastPriority = 100
  highestPriority = 101
  lastChangeTick = 0
  lastTick = 0

  def __init__(self,cmd,host):
    self.state="INIT"
    self.lastState="INIT"
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
    syslog.syslog("Thread start - ID=%s host=%s command=\"%s\" monitoring=%s priority=%s" % (self.ID, self.host.name, self.cmd, self.monitorHosts, self.priority))
    self.runTask()
    syslog.syslog("Thread end - ID=%s host=%s command=\"%s\" priority=%s" % (self.ID, self.host.name, self.cmd, self.priority))

  def addMonitorHost(self,host):
    self.monitorHosts.append(host)

  def runTask(self):
    while self.state != "COMPLETE":
      self.transition()
#      print "THREAD %s HEARTBEAT - %s" % (self.ID,self.state)
      self.loaded = False
      for host in self.monitorHosts:
        for load in self.host.loads:
          if self.host.loads[load] > self.host.limits[load]:
            self.loaded = True
#            print "Task running on loaded host"

        if (TaskRunner.lastTick - TaskRunner.lastChangeTick) >= 3:
          if self.state == "PAUSED" and self.loaded == False:
            self.resumeTask()
          if (TaskRunner.highestPriority == self.priority):
            if self.state == "RUNNING" and self.loaded == True:
              self.pauseTask()
          TaskRunner.lastChangeTick = TaskRunner.lastTick
#          print "State change window reached"

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
    self.streamOut = StreamReader(self.process.stdout)
    self.streamErr = StreamReader(self.process.stderr)
#    print "PID: %s" % self.process.pid

  def transition(self):
    self.lastState=self.state
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
      while True:
        line = self.streamOut.readline(0.1)
        if not line:
          break
        message = "Thread stdout - ID=%s output=\"%s\"" % (self.ID,line)
        syslog.syslog(message)
#        print message

      while True:
        line = self.streamErr.readline(0.1)
        if not line:
          break
        message = "Thread stderr - ID=%s output=\"%s\"" % (self.ID,line)
        syslog.syslog(message)
#        print message

      if retcode is not None:
        self.completeTask()

    if self.state == "PAUSING":
      self.state = "PAUSED"

    if self.state == "RESUMING":
      self.state = "RUNNING"

    if self.state != self.lastState:
      message = "Thread transition - ID=%s old_state=%s new_state=%s" % (self.ID,self.lastState,self.state)
      print message
      syslog.syslog(message)

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
