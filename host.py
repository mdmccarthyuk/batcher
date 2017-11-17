#!/usr/bin/python

import sys


class Host:
    def __init__(self, name):
        self.name = name
        self.method = "local"
        self.user = ""
        self.limits = dict()
        self.loads = dict()
        self.lowerLimits = dict()
        self.loads['load'] = 0.0
        self.loads['iowait'] = 0
        self.limits['load'] = 1.0
        self.limits['iowait'] = 0.5
        self.lowerLimits['load'] = 1.0
        self.lowerLimits['iowait'] = 0.5
        self.abort = 100.0
        self.lastCpuStat = []
        self.lastCpuTotal = 0

    def sshAccess(self, user):
        self.method = "ssh"
        self.user = user


if __name__ == "__main__":
    sys.exit(0)
