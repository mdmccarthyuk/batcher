#!/usr/bin/python

import sys
import time
import argparse
import sqlite3
import os
import json
from subprocess import Popen, PIPE
from daemon import Daemon
from task import TaskRunner
from host import Host

check_running = dict()
check_result = dict()
running_tasks = dict()
host_list = dict()
abort_flag = False


class BatcherDaemon(Daemon):
    def run(self):
        print("STARTING")
        main(None)


def main(args):
    global check_running, check_result, debug_flag, host_list, running_tasks, abort_flag

    task_count = 0
    tick_count = 0

    worker_get_hosts()
    worker_get_tasks()

    if args.nuke:
        for task in running_tasks:
            task_done(task)
            if running_tasks[task].state == "RUNNING":
                running_tasks[task].stop_task()
                running_tasks[task].kill_task()
        sys.exit(0)

    if args.kill:
        pipe_out = os.open('/var/run/batcher/batcher', os.O_RDWR)
        os.write(pipe_out, "QUIT")
        os.close(pipe_out)
        sys.exit(0)

    if not os.path.exists('/var/run/batcher'):
        os.mkdir('/var/run/batcher')

    print("Making FIFO")
    if os.path.exists('/var/run/batcher/batcher'):
        os.remove('/var/run/batcher/batcher')

    os.mkfifo('/var/run/batcher/batcher')
    pipe_in = os.open('/var/run/batcher/batcher', os.O_RDONLY | os.O_NONBLOCK)
    pipe_read = ""

    if args.file is not None:
        print("Parsing " + args.file)
        with open(args.file) as jobFile:
            configuration = json.load(jobFile)
        task_max = configuration['batcherConfig']['core']['maxTasks']
        for confTask in configuration['batcherConfig']['job']['tasks']:
            killable_flag = False
            if confTask['killable'] == 1:
                killable_flag = True
            if confTask['priority'] > 100:
                print("ERROR> Step priority can't be greater than 100")
                sys.exit(1)
            if 'sshuser' in confTask:
                new_task_user = confTask['sshuser']
            else:
                new_task_user = 'DEFAULT'
            task_add(confTask['command'], confTask['host'], confTask['monitor'], killable_flag, confTask['priority'],
                     new_task_user)
        for confHost in configuration['batcherConfig']['job']['hosts']:
            if confHost['name'] not in host_list:
                host_add(confHost['name'], confHost['type'], confHost['user'])
            for limit in confHost['metrics']['upper']:
                host_limit(confHost['name'], limit, confHost['metrics']['upper'][limit], False)
            for limit in confHost['metrics']['lower']:
                host_limit(confHost['name'], limit, confHost['metrics']['lower'][limit], True)
            host_limit(confHost['name'], 'abort', confHost['metrics']['abort']['load'], False)
    else:
        task_max = 2

    while pipe_read != "QUIT":
        #    print "main> Heartbeat %s (Tasks %s/%s) " % (tick_count,task_count,taskMax)

        worker_get_hosts()
        worker_get_tasks()

        for host in host_list:
            host_check_load(host_list[host])

        complete_tasks = []
        lowest_priority = 101
        next_lowest_priority = 101
        new_highest_priority = 0
        new_lowest_paused_priority = 101
        paused_tasks = False

        for task in running_tasks:
            if running_tasks[task].priority < lowest_priority:
                lowest_priority = running_tasks[task].priority
            if running_tasks[task].state == 'INIT':
                if running_tasks[task].priority < next_lowest_priority:
                    next_lowest_priority = running_tasks[task].priority
            if running_tasks[task].priority > new_highest_priority:
                if running_tasks[task].state == 'RUNNING':
                    new_highest_priority = running_tasks[task].priority
            if running_tasks[task].state == 'PAUSED':
                paused_tasks = True
                if running_tasks[task].priority < new_lowest_paused_priority:
                    new_lowest_paused_priority = running_tasks[task].priority

        TaskRunner.highestPriority = new_highest_priority
        TaskRunner.lowestPausedPriority = new_lowest_paused_priority

        for task in running_tasks:
            if running_tasks[task].state == 'INIT':
                if running_tasks[task].priority == next_lowest_priority:
                    if not paused_tasks:
                        if task_count < task_max:
                            task_count += 1
                            running_tasks[task].start()
            if running_tasks[task].state in ["COMPLETING", "COMPLETE"]:
                complete_tasks.append(task)
                task_count -= 1
                task_done(task)

        for task in complete_tasks:
            del running_tasks[task]

        time.sleep(5)
        tick_count += 1
        TaskRunner.lastTick = tick_count
        pipe_read = os.read(pipe_in, 1024).strip()
        if len(running_tasks) == 0 and tick_count > 1:
            pipe_read = "QUIT"
            print("All tasks completed")

        if abort_flag:
            print("ABORTING")
            pipe_read = "QUIT"
            for task in running_tasks:
                if running_tasks[task].state == "RUNNING":
                    running_tasks[task].kill_task()
                    running_tasks[task].transition()
                running_tasks[task].complete_task()
                running_tasks[task].transition()

    print("Exiting")
    os.close(pipe_in)
    os.remove('/var/run/batcher/batcher')
    conn = sqlite3.connect('/var/run/batcher/core.db')
    c = conn.cursor()
    c.execute('delete from tasks where status=\'DONE\'')
    conn.commit()
    conn.close()


def check_for_db():
    conn = sqlite3.connect('/var/run/batcher/core.db')
    sql = 'create table if not exists hosts (id INTEGER PRIMARY KEY, hostname text, access text,' \
          ' user text, limit_load FLOAT, limit_iowait FLOAT, lower_load FLOAT, lower_iowait FLOAT, limit_abort FLOAT)'
    c = conn.cursor()
    c.execute(sql)
    conn.commit()
    sql = 'create table if not exists tasks (id INTEGER PRIMARY KEY, status text, task text, time text, ' \
          'host text, pid INTEGER, monitor TEXT, killable INTEGER, priority INTEGER, sshuser text)'
    c.execute(sql)
    conn.commit()
    conn.close()


def worker_get_hosts():
    global host_list
    conn = sqlite3.connect('/var/run/batcher/core.db')
    sql = 'select * from hosts'
    c = conn.cursor()
    c.execute(sql)
    row = c.fetchone()
    while row is not None:
        if row[1] not in host_list:
            new_host = Host(row[1])
            if row[2] == 'ssh':
                new_host.sshAccess(row[3])
            new_host.limits['load'] = float(row[4])
            new_host.limits['iowait'] = float(row[5])
            new_host.lowerLimits['load'] = float(row[6])
            new_host.lowerLimits['iowait'] = float(row[7])
            new_host.abort = float(row[8])
            host_list[row[1]] = new_host
        else:
            host_list[row[1]].limits['load'] = float(row[4])
            host_list[row[1]].limits['iowait'] = float(row[5])
            host_list[row[1]].lowerLimits['load'] = float(row[6])
            host_list[row[1]].lowerLimits['iowait'] = float(row[7])
            host_list[row[1]].abort = float(row[8])

        row = c.fetchone()
    conn.close()


def worker_get_tasks():
    global running_tasks, host_list
    conn = sqlite3.connect('/var/run/batcher/core.db')
    c = conn.cursor()
    sql = 'SELECT id,status,task,time,host,pid,monitor,killable,priority,sshuser FROM tasks WHERE NOT status=\'DONE\''
    c.execute(sql)
    row = c.fetchone()
    while row is not None:
        if row[0] not in running_tasks:
            host_names = row[6]
            hosts = host_names.split(',')
            new_task = TaskRunner(row[2], host_list[row[4]])
            if row[7] == 1:
                new_task.killable = True
            for host in hosts:
                if host in host_list:
                    new_task.add_monitor_host(host_list[host])
                else:
                    print("ERROR - Unknown monitor host %s" % host)
                    sys.exit(0)
            new_task.priority = row[8]
            new_task.remoteRunAs = row[9]
            running_tasks[row[0]] = new_task
        row = c.fetchone()
    conn.close()


def cmd_host(args):
    if args.list:
        print("HOST LIST")
        host_get_list()
        sys.exit(0)
    if args.limit is not None:
        if not args.metric:
            print("Missing --metric")
            sys.exit(1)
        if not args.value:
            print("Missing --value")
            sys.exit(1)
        host_limit(args.limit, args.metric, args.value, False)
        sys.exit(0)
    if args.lowerlimit is not None:
        if args.metric:
            print("Missing --metric")
            sys.exit(1)
        if not args.value:
            print("Missing --value")
            sys.exit(1)
        host_limit(args.lowerlimit, args.metric, args.value, True)

    if args.add is not None:
        print("HOST ADD")
        host_add(args.add, args.method, args.user)
        sys.exit(0)
    if args.delete is not None:
        print("HOST DELETE")
        host_delete(args.delete)
        sys.exit(0)


def cmd_service(args):
    daemon = BatcherDaemon('/var/run/batcher/batcher.pid')
    print("cmd_service> entered")
    if args.action == 'start':
        print("cmd_service> daemon starting")
        daemon.start()
        print("cmd_service> daemon started")
        sys.exit(0)
    elif args.action == 'stop':
        daemon.stop()
        sys.exit(0)
    elif args.action == 'restart':
        daemon.restart()
        sys.exit(0)
    else:
        print("Invalid service command\n")
        sys.exit(1)


def cmd_task(args):
    if args.list:
        task_get_list()
        sys.exit(0)
    if args.add is not None:
        if args.host == 'null':
            print("Host for task not specified")
            sys.exit(1)
        task_add(args.add, args.host, args.monitor, args.killable, args.priority, 'DEFAULT')
        sys.exit(0)


def cmd_worker(args):
    global workerStatus
    if args.status:
        if workerStatus == "STOP":
            print("Worker is stopped")
        elif workerStatus == "START":
            print("Worker is started")
        else:
            print("Worker is in state %s" % workerStatus)
    if args.go:
        workerStatus = "START"
    if args.stop:
        workerStatus = "STOP"
    sys.exit(0)


def task_get_list():
    conn = sqlite3.connect('/var/run/batcher/core.db')
    sql = 'select * from tasks'
    c = conn.cursor()
    c.execute(sql)
    row = c.fetchone()
    while row is not None:
        print(row)
        row = c.fetchone()
    conn.close()


def task_add(task, host, monitor, killable, priority, sshuser):
    conn = sqlite3.connect('/var/run/batcher/core.db')
    c = conn.cursor()
    kill_val = 1 if killable else 0
    c.execute('INSERT INTO tasks (status, task, host, monitor, killable, priority, sshuser) values '
              '(\'init\', ?, ?, ?, ?, ?, ?)',
              (task, host, monitor, kill_val, priority, sshuser))
    conn.commit()
    conn.close()


def task_done(task):
    conn = sqlite3.connect('/var/run/batcher/core.db')
    c = conn.cursor()
    c.execute('UPDATE tasks SET status=\'DONE\' WHERE id = ?', (task,))
    conn.commit()
    conn.close()


def host_get_list():
    conn = sqlite3.connect('/var/run/batcher/core.db')
    sql = 'select hostname from hosts'
    c = conn.cursor()
    c.execute(sql)
    row = c.fetchone()
    while row is not None:
        print(row[0])
        row = c.fetchone()
    conn.close()


def host_add(hostname, method, user):
    if method == 'ssh':
        access_type = 'ssh'
        access_user = user
    elif method == 'local':
        access_type = 'local'
        access_user = 'local'
    else:
        print("host_add> invalid access Type - %s" % method)
        sys.exit(1)
    conn = sqlite3.connect('/var/run/batcher/core.db')
    c = conn.cursor()
    c.execute(
        'INSERT INTO HOSTS (hostname, access, user, limit_load, limit_iowait, lower_load, lower_iowait)'
        ' values (?, ?, ?, 1.0, 0.5, 1.0, 0.5)',
        (hostname, access_type, access_user))
    conn.commit()
    conn.close()


def host_delete(hostname):
    conn = sqlite3.connect('/var/run/batcher/core.db')
    c = conn.cursor()
    c.execute(
        'DELETE FROM HOSTS where hostname=?', hostname)
    conn.commit()
    conn.close()


def host_limit(hostname, limit_name, limit, is_lower):
    allowed_limits = ['load', 'iowait', 'abort']
    if is_lower:
        prefix = "lower_"
    else:
        prefix = "limit_"
    if limit_name not in allowed_limits:
        print("Unknown limit name %s" % limit_name)
        sys.exit(1)
    else:
        sql_limit = prefix + limit_name
    conn = sqlite3.connect('/var/run/batcher/core.db')
    c = conn.cursor()
    print("%s %s %s" % (sql_limit, limit, hostname))
    c.execute("UPDATE hosts SET %s=%s WHERE hostname = ?" % (sql_limit, limit), (hostname,))
    conn.commit()
    conn.close()


def host_check_load(host):
    global check_running, check_result, abort_flag
    if host.method == "ssh":
        command = "ssh %s@%s 'cat /proc/loadavg /proc/stat'" % (host.user, host.name)
    elif host.method == "local":
        command = "cat /proc/loadavg /proc/stat"
    else:
        print("host_checkLoad> Unknown access method")
        sys.exit(1)

    if host.name not in check_running:
        #    print "host_checkLoad> %s check triggered" % host.name
        check_running[host.name] = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
    else:
        taskproc = check_running[host.name]
        retcode = taskproc.poll()
        if retcode is not None:
            check_result[host.name] = taskproc.stdout.read()
            del check_running[host.name]
            lines = check_result[host.name].split('\n')
            loads = lines[0].split()
            cpu_stat = lines[1].split()
            cpu_total = 0
            del (cpu_stat[0])
            for cpuVal in cpu_stat:
                cpu_total += int(cpuVal)
            if len(host.lastCpuStat):
                delta_total = cpu_total - host.lastCpuTotal
                delta_wait = int(cpu_stat[4]) - int(host.lastCpuStat[4])
                #        print "%s %s %s %s" % (delta_total, delta_wait, cpu_stat[4], host.lastCpuStat[4])
                host.loads['iowait'] = float(delta_wait) / float(delta_total)
            else:
                host.loads['iowait'] = 0.0
            host.lastCpuStat = cpu_stat
            host.lastCpuTotal = 0
            for cpuVal in host.lastCpuStat:
                host.lastCpuTotal += int(cpuVal)
            host.loads['load'] = float(loads[0])
            print("host_checkLoad> %s load is %s Limits %s %s" % (
              host.name, host.loads['load'], host.limits['load'], host.lowerLimits['load']))
            print("host_checkLoad> %s IOWait is %s" % (host.name, host.loads['iowait']))
        else:
            print("host_checkLoad> %s check still running" % host.name)
        print(host.abort)
        if host.loads['load'] > host.abort:
            abort_flag = True
            print("Abort flag set due to host %s" % host.name)


if __name__ == "__main__":
    workerStatus = "START"
    debug_flag = False
    check_for_db()
    parser = argparse.ArgumentParser(description="Batcher core")
    subparsers = parser.add_subparsers()

    parser_standalone = subparsers.add_parser('standalone')
    parser_standalone.add_argument('-d', '--debug', action='store_true')
    parser_standalone.add_argument('-k', '--kill', action='store_true')
    parser_standalone.add_argument('-n', '--nuke', action='store_true')
    parser_standalone.add_argument('-f', '--file')
    parser_standalone.set_defaults(func=main)

    parser_daemon = subparsers.add_parser('service')
    parser_daemon.add_argument('-k', '--action')
    parser_daemon.add_argument('-d', '--debug', action='store_true')
    parser_daemon.set_defaults(func=cmd_service)

    parser_host = subparsers.add_parser('host')
    parser_host.add_argument('-l', '--list', action='store_true')
    parser_host.add_argument('-a', '--add')
    parser_host.add_argument('-m', '--method', default='ssh')
    parser_host.add_argument('-u', '--user')
    parser_host.add_argument('-d', '--delete')
    parser_host.add_argument('--limit')
    parser_host.add_argument('--lowerlimit')
    parser_host.add_argument('--value')
    parser_host.add_argument('--metric')
    parser_host.set_defaults(func=cmd_host)

    parser_task = subparsers.add_parser('task')
    parser_task.add_argument('-l', '--list', action='store_true')
    parser_task.add_argument('-H', '--host', default="null")
    parser_task.add_argument('-m', '--monitor')
    parser_task.add_argument('-a', '--add')
    parser_task.add_argument('-k', '--killable')
    parser_task.add_argument('-p', '--priority', default=100)
    parser_task.set_defaults(func=cmd_task)

    parser_worker = subparsers.add_parser('worker')
    parser_worker.add_argument('-g', '--go', action='store_true')
    parser_worker.add_argument('-s', '--stop', action='store_true')
    parser_worker.add_argument('--status', action='store_true')
    parser_worker.set_defaults(func=cmd_worker)

    parsed_args = parser.parse_args()
    parsed_args.func(parsed_args)
    sys.exit(0)
