#! /usr/bin/env python

from subprocess import Popen as sp_Popen, PIPE as sp_PIPE
from multiprocessing import Pool as mp_Pool, Event as mp_Event, cpu_count as mp_cpu_count, Lock as mp_Lock
from sys import stdin as sys_stdin, argv as sys_argv, exit as sys_exit
from argparse import ArgumentParser as arg_ArgumentParser

SSH_ROOT_CMD = "ssh -o StrictHostKeyChecking=no -o LogLevel=quiet -o ConnectTimeout=%s"
SSH_ConnectTimeout_DEFAULT = 2

OUTPUT_SEPARATOR_DEAFULT = ","
OUTPUT_FIELDS = "hostname returncode stdout_stderr stdout stderr status command"
OUTPUT_FORMAT_DEAFULT = "hostname returncode stdout_stderr"

def format_output(result, output_separator, result_format):
    if "stdout_stderr" in result_format:
        result["stdout_stderr"] = "\n".join([ r for r in [result["stdout"],
            result["stderr"]] if r])
    return output_separator.join([str(result[feild]) for feild in result_format])

def ssh((hostname, command, ssh_timeout, realtime, output_separator,
    result_format)):
    result = {
                "returncode": None,
                "stdout": None,
                "stderr": None,
                "command": "%s %s %s" % (SSH_ROOT_CMD % ssh_timeout, hostname,
                    command),
                "hostname": hostname,
                "status": "pending",
                }

    process = sp_Popen("%s %s %s" % (SSH_ROOT_CMD % ssh_timeout, hostname, command), 
        stdout=sp_PIPE, stderr=sp_PIPE, shell=True)    
    try:
        if not terminating.is_set():
            stdout,stderr = process.communicate()
            result["returncode"] = process.returncode
            result["stdout"] = stdout.strip("\n")
            result["stderr"] = stderr.strip("\n")

            result["status"] = "done"
        else:
            result["returncode"] = 2
            result["status"] = "aborted"
    except KeyboardInterrupt:
        result["returncode"] = 1
        result["status"] = "killed"
        terminating.set()
        process.kill()

    if realtime:
        lock.acquire()
        print format_output(result, output_separator, result_format)
        lock.release()

    return result

def initializer(terminating_, lock_):
    # This places terminating in the global namespace of the worker subprocesses.
    # This allows the worker function to access `terminating` even though it is
    # not passed as an argument to the function.
    global terminating
    terminating = terminating_

    global lock
    lock = lock_

class Main(object):
    def __init__(self):
        parser = arg_ArgumentParser(description="")
        parser.add_argument("-c", "--command", dest="command", required=True,
            help="a command to run on remote hosts")
        parser.add_argument("-t", "--ssh_timeout", dest="ssh_timeout", 
            default=SSH_ConnectTimeout_DEFAULT,
            help="ssh ConnectTimout value (DEFAULT: %s)" % SSH_ConnectTimeout_DEFAULT)
        parser.add_argument("-p", "--parallel_processes", dest="processes",
            default=None,
            help="parallel processes to run (DEFAULT: %s)" % mp_cpu_count())
        parser.add_argument("-o", "--output_separator", dest="output_separator",
            default=OUTPUT_SEPARATOR_DEAFULT,
            help="separator for output (DEFAULT: \"%s\")" % OUTPUT_SEPARATOR_DEAFULT) 
        parser.add_argument("-f", "--output_format", dest="output_format",
            nargs='+', default=OUTPUT_FORMAT_DEAFULT.split(" "),
            choices=OUTPUT_FIELDS.split(" "),
            help="output fields (DEFAULT: \"%s\")" % OUTPUT_FORMAT_DEAFULT)       
        parser.add_argument("-q", "--quiet", dest="realtime",
            action="store_false",
            help="do not output results as they are recieved.")
        self.args = parser.parse_args()

        self.stdin = sys_stdin.readlines()
        self.host_list = [ h.strip("\n") for h in self.stdin if self.check_host_is_valid(h) ]

    def check_host_is_valid(self, host):
        return True

    def check_hostlist_is_valid(self):
        if len(self.host_list) > 0:
            return True
        else:
            sys_exit("Invalid host list provided")

    def run(self,):
        terminating = mp_Event()
        lock = mp_Lock()
        results = []
        worker_pool = mp_Pool(processes=self.args.processes,
            initializer=initializer, initargs=(terminating, lock))
        worker_args = [ (host, self.args.command, self.args.ssh_timeout, 
            self.args.realtime,self.args.output_separator,
            self.args.output_format) for host in self.host_list ]

        try:
            results = worker_pool.map(ssh, worker_args)
            worker_pool.close()
        except KeyboardInterrupt:
            worker_pool.terminate()
        finally:
            worker_pool.join()

        if not self.args.realtime:
            for result in results:
                print format_output(result, self.args.output_separator,
                    self.args.output_format)

if __name__ == "__main__":
    m = Main()
    m.check_hostlist_is_valid()
    m.run()
