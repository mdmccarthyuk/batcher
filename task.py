#!/usr/bin/python

import os
import signal
import threading
import time
import sys
import syslog
from subprocess import Popen, PIPE
from stream import StreamReader


class TaskRunner(threading.Thread):
    syslog.openlog('batcher')

    nextTaskID = 0
    lastPriority = 100
    lowestPausedPriority = 100
    highestPriority = 1
    lastTick = 0
    lastAllChangeTick = 0

    def __init__(self, cmd, host):
        self.state = "INIT"
        self.lastState = "INIT"
        TaskRunner.nextTaskID += 1
        self.ID = TaskRunner.nextTaskID
        self.killable = False
        self.loaded = False
        self.underLimit = False
        self.host = host
        self.monitorHosts = []
        self.cmd = cmd
        self.priority = 100
        threading.Thread.__init__(self)
        self.lastChangeTick = 0
        self.remoteRunAs = "DEFAULT"
        self.process = None
        self.streamOut = None
        self.streamErr = None

    def run(self):
        message = "Thread start - ID=%s host=%s command=\"%s\" monitoring=%s priority=%s" % (
          self.ID, self.host.name, self.cmd, self.monitorHosts, self.priority)
        print(message)
        syslog.syslog(message)
        self.run_task()
        syslog.syslog("Thread end - ID=%s host=%s command=\"%s\" priority=%s" % (
          self.ID, self.host.name, self.cmd, self.priority))

    def add_monitor_host(self, host):
        self.monitorHosts.append(host)

    def run_task(self):
        while self.state != "COMPLETE":
            self.transition()
            print("THREAD %s HEARTBEAT - %s priority %s" % (self.ID, self.state, self.priority))
            self.loaded = False
            self.underLimit = True
            for host in self.monitorHosts:
                for load in host.loads:
                    if host.loads[load] > host.limits[load]:
                        self.loaded = True
                    if host.loads[load] > host.lowerLimits[load]:
                        self.underLimit = False

            if (TaskRunner.lastTick - TaskRunner.lastAllChangeTick) >= 1:
                if self.state == "PAUSED" and self.underLimit:
                    if TaskRunner.lowestPausedPriority == self.priority:
                        self.resume_task()
                        TaskRunner.lastAllChangeTick = TaskRunner.lastTick
                if TaskRunner.highestPriority == self.priority:
                    if self.state == "RUNNING" and self.loaded:
                        self.pause_task()
                        TaskRunner.lastAllChangeTick = TaskRunner.lastTick
                self.lastChangeTick = TaskRunner.lastTick

            time.sleep(1)

    def start_task(self):
        if self.host.method == 'ssh':
            if self.remoteRunAs == 'DEFAULT':
                task_user = self.host.user
            else:
                task_user = self.remoteRunAs
            command = "ssh -t %s@%s 'stty -echo -onlcr; %s; stty echo onlcr'" % (task_user, self.host.name, self.cmd)
        elif self.host.method == 'local':
            command = self.cmd
        else:
            print("task method invalid")
            sys.exit(1)
        print("Starting streams")
        self.process = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
        self.streamOut = StreamReader(self.process.stdout)
        self.streamErr = StreamReader(self.process.stderr)
        print("PID: %s" % self.process.pid)
        os.system('stty sane')

    def transition(self):
        self.lastState = self.state
        if self.state == "INIT":
            self.start_task()
            self.state = "STARTING"

        if self.state == "STARTING":
            self.state = "RUNNING"

        if self.state == "COMPLETING":
            self.state = "COMPLETE"

        if self.state == "RUNNING":
            taskproc = self.process
            retcode = taskproc.poll()

            while True:
                line = self.streamOut.readline(0.1)
                if not line:
                    break
                message = "Thread stdout - ID=%s output=\"%s\"" % (self.ID, line)
                syslog.syslog(message)
                print(message)

            while True:
                line = self.streamErr.readline(0.1)
                if not line:
                    break
                message = "Thread stderr - ID=%s output=\"%s\"" % (self.ID, line)
                syslog.syslog(message)
                print(message)

            if retcode is not None:
                self.complete_task()

        if self.state == "PAUSING":
            self.state = "PAUSED"

        if self.state == "RESUMING":
            self.state = "RUNNING"

        if self.state != self.lastState:
            message = "Thread transition - ID=%s old_state=%s new_state=%s" % (self.ID, self.lastState, self.state)
            print(message)
            syslog.syslog(message)

    def pause_task(self):
        self.state = "PAUSING"
        if self.host.method == "local":
            if self.killable:
                os.kill(self.process.pid, signal.SIGTERM)
            else:
                os.kill(self.process.pid, signal.SIGSTOP)
        elif self.host.method == "ssh":
            if self.killable:
                os.kill(self.process.pid, signal.SIGTERM)
            else:
                print("ssh: pause not implemented")
        else:
            print("Invalid host method")

    def resume_task(self):
        self.state = "RESUMING"
        if self.host.method == "local":
            if self.killable:
                self.start_task()
            else:
                os.kill(self.process.pid, signal.SIGCONT)
        elif self.host.method == "ssh":
            if self.killable:
                self.start_task()
            else:
                print("ssh: resume not implemented")
        else:
            print("Invalid host method")

    def kill_task(self):
        self.killable = True
        self.pause_task()

    def stop_task(self):
        self.state = "STOPPING"

    def complete_task(self):
        self.state = "COMPLETING"


if __name__ == "__main__":
    sys.exit(0)
