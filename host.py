#!/usr/bin/python

import sys

class Host:
  def __init__(self,name):
    self.name = name
    self.method = "local"
    self.limits = dict()
    self.loads = dict()
    self.loads['load'] = 0.0
    self.loads['iowait'] = 0
    self.limits['load'] = 1.0
    self.limits['iowait'] = 50.0

  def sshAccess(self,user):
    self.method = "ssh"
    self.user = user

if __name__ == "__main__":
  sys.exit(0)
