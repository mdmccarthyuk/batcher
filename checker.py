#!/usr/bin/python

import sys
import argparse
import json
import time
import os

def main():
  global args

  if(args.type == 'load'):
    checkLoad(args.upper)

def checkLoad(upper):
  print "Checking load averages"
  load = os.getloadavg()[0]
  if float(load) > float(args.upper):
    print "Load BAD - is %s - alert on %s" % ( load, args.upper )
    sys.exit(1)
  else: 
    print "Load OK - is %s - alert on %s" % ( load, args.upper )
    sys.exit(0)

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="Batcher node checker")
  parser.add_argument('-t','--type',required=True,help='Type of check')
  parser.add_argument('-l','--lower',help='Low limit to alert on')
  parser.add_argument('-u','--upper',help='High limit to alert on')
    
  args = parser.parse_args()
  main()
  sys.exit(0)
