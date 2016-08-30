#!/usr/bin/python

import sys
from subprocess import Popen, PIPE
from host import Host


class TaskRunner:

  nextTaskID = 0

  def __init__(self,cmd,host):
    self.state="INIT"
    TaskRunner.nextTaskID+=1
    self.ID=TaskRunner.nextTaskID
    self.host = host
    self.cmd = cmd

  def start(self, task):
    self.state="STARTING"
    self.task=task
    print "Starting task %s" % task 
    if self.host != 'local':
      command = "ssh %s@%s '%s'" % (

  def stop(self, taskID):
    self.state="STOPPING"
    print "Stopping task %s %s" % (self.ID, self.task)

if __name__ == "__main__":

  sys.exit(0)

