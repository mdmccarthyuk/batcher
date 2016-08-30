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

  def start(self):
    self.state="STARTING"
    if self.host.method == 'ssh':
      command = "ssh %s@%s '%s'" % ( self.host.user, self.host.name, self.cmd)
    elif self.host.method == 'local':
      command = self.cmd
    else:
      print "task method invalid"
      sys.exit(1)
    self.process = Popen(command,shell=True,stdout=PIPE,stderr=PIPE)

  def stop(self, taskID):
    self.state="STOPPING"

  def complete(self):
    self.state="COMPLETING"

if __name__ == "__main__":

  sys.exit(0)

