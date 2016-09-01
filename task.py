#!/usr/bin/python

import os, signal, threading, time, sys
from subprocess import Popen, PIPE
from host import Host

class TaskRunner (threading.Thread):

  nextTaskID = 0

  def __init__(self,cmd,host):
    self.state="INIT"
    TaskRunner.nextTaskID+=1
    self.ID=TaskRunner.nextTaskID
    self.host = host
    self.hosts = [ host ]
    self.cmd = cmd
    threading.Thread.__init__(self)

  def run(self):
    print "Starting thread"
    self.runTask()
    print "Exiting thread"

  def addHost(self,host):
    self.hosts.append(host)

  def runTask(self):
    while self.state != "COMPLETE":
      self.transition()
      print "THREAD HEARTBEAT - %s" % self.state

      if self.host.loads['load'] > 0.30:
        print "Task running on loaded host"
        self.pauseTask()

      if self.host.loads['load'] < 0.15:
        if self.state == "PAUSED":
          self.resumeTask()

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
      os.kill(self.process.pid, signal.SIGSTOP)
    elif self.host.method == "ssh":
      print "ssh: pause IOU"
    else:
      print "Invalid host method"

  def resumeTask(self):
    self.state="RESUMING"
    if self.host.method == "local":
      os.kill(self.process.pid, signal.SIGCONT)
    elif self.host.method == "ssh":
      print "ssh: resume IOU"
    else:
      print "Invalid host method"

  def stopTask(self):
    self.state="STOPPING"

  def completeTask(self):
    self.state="COMPLETING"

if __name__ == "__main__":

  sys.exit(0)
