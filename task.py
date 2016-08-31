#!/usr/bin/python

import sys
import time
import threading
from subprocess import Popen, PIPE
from host import Host


class TaskRunner (threading.Thread):

  nextTaskID = 0

  def __init__(self,cmd,host):
    self.state="INIT"
    TaskRunner.nextTaskID+=1
    self.ID=TaskRunner.nextTaskID
    self.host = host
    self.cmd = cmd
    threading.Thread.__init__(self)

  def run(self):
    print "Starting thread"
    self.runTask()
    print "Exiting thread"

  def runTask(self):
    while self.state != "COMPLETE":
      self.transition()
      print "THREAD HEARTBEAT - %s" % self.state
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

  def pauseTask(self):
    self.state="PAUSING"

  def stopTask(self):
    self.state="STOPPING"

  def completeTask(self):
    self.state="COMPLETING"

if __name__ == "__main__":

  sys.exit(0)

