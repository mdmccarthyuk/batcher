#!/usr/bin/python

import sys

class Host:
  def __init__(self,name,method,user):
    self.name = name
    self.method = method
    self.user = user

  def __init__(self,name):
    self.name = name
    self.method = "local"

if __name__ == "__main__":
  sys.exit(0)
