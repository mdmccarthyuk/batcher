#!/usr/bin/python

import sys

class Host:
  def __init__(self,name):
    self.name = name
    self.method = "local"

  def sshAccess(self,user):
    self.method = "ssh"
    self.user = user

if __name__ == "__main__":
  sys.exit(0)
