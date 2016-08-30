#!/usr/bin/python

import sys
from subprocess import Popen, PIPE

class TaskRunner:

  nextTaskID = 0

  def __init__(self):
    self.state="INIT"
    self.taskID=1

  def runTask(self, task):
    self.state="STARTING"
    self.task=task
    print "Starting task %s" % task 

  def stopTask(self, taskID):
    self.state="STOPPING"
    print "Stopping task %s %s" % (self.taskID, self.task)

  def status(self):
    return self.state

if __name__ == "__main__":

  sys.exit(0)

