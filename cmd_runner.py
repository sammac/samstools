#! /usr/bin/env python

from sys import stdin as sys_stdin, exit as sys_exit
from multiprocessing import (Pool as mp_Pool, Event as mp_Event,
    cpu_count as mp_cpu_count)
from subprocess import Popen as sp_Popen, PIPE as sp_PIPE
from argparse import ArgumentParser as arg_ArgumentParser

# Script for running concurrent remote commands using multiprocessing,
# subprocess and SSH. Output can be customised and results provided in realtime
# or after exectuion on all nodes.

# SSH defaults
SSH_EXEC = "/usr/bin/ssh"
SSH_OPTS = "-o StrictHostKeyChecking=no -o LogLevel=quiet -o ConnectTimeout=%s"
SSH_CMD = "%s %s" % (SSH_EXEC,SSH_OPTS)
SSH_ConnectTimeout_DEFAULT = 2

# Outputting options and field descriptions.
OUTPUT_SEPARATOR_DEAFULT = ","
OUTPUT_FIELDS = "hostname returncode stdout_stderr stdout stderr status command"
OUTPUT_FORMAT_DEAFULT = "hostname returncode stdout_stderr"

def ssh((hostname, command, ssh_timeout, realtime, multiline)):
    """Takes a tuple of arguments as multiprocessing map only accepts one and 
    returns a result dictionary."""
    result = {
                "returncode": None,
                "stdout": None,
                "stderr": None,
                "command": "%s %s %s" % (SSH_CMD % ssh_timeout, hostname,
                    command),
                "hostname": hostname,
                "status": "pending",
                }

    # Run in a try/catch to check for SIGINT, this will kill the running process
    # then set the global terminating event preventing subsequent workers from
    # running.
    try:
        if terminating.is_set():
            # If we are terminating, update the result set and return.
            result["returncode"] = 2
            result["status"] = "aborted"
        else:
            # Start subprocess for ssh. shell=True is nasty, but this is needed.
            process = sp_Popen("%s %s %s" % (SSH_CMD % ssh_timeout, hostname, 
                command), stdout=sp_PIPE, stderr=sp_PIPE, shell=True)
            stdout,stderr = process.communicate()

            if multiline:
                result["stdout"] = stdout.strip("\n")
                result["stderr"] = stderr.strip("\n")
            else:
                result["stdout"] = stdout.replace("\n", " ")
                result["stderr"] = stderr.replace("\n", " ")
            result["returncode"] = process.returncode
            result["status"] = "done"

    except KeyboardInterrupt:
        # Catch keyboard interrupt, killing the current worker and setting the
        # global terminating event.
        result["returncode"] = 1
        result["status"] = "killed"
        terminating.set()
        process.kill()

    return result

def initializer(terminating_):
    # This places terminating and lock in the global namespace of the worker 
    # subprocesses. This allows the worker function to access `terminating` and 
    # `lock`s even though it is not passed as an argument to the function.
    global terminating
    terminating = terminating_

class Main(object):
    def __init__(self):
        """Parses arguments and the hostlist from stdin."""
        parser = arg_ArgumentParser(description="""Run commands in parallel 
            across a node list given to stdin.""")
        parser.add_argument("-c", "--command", dest="command", required=True,
            help="a command to run on remote hosts")
        parser.add_argument("-t", "--ssh_timeout", dest="ssh_timeout", 
            default=SSH_ConnectTimeout_DEFAULT,
            help="ssh ConnectTimout value DEFAULT: %s" % (
                SSH_ConnectTimeout_DEFAULT))
        parser.add_argument("-p", "--parallel_processes", dest="processes",
            default=mp_cpu_count()*2,type=int,
            help="parallel processes DEFAULT: %s (cpu_count * 2)" % (
                mp_cpu_count()*2))
        parser.add_argument("-o", "--output_separator", dest="output_separator",
            default=OUTPUT_SEPARATOR_DEAFULT,
            help="separator for output DEFAULT: \"%s\"" % (
                OUTPUT_SEPARATOR_DEAFULT)) 
        parser.add_argument("-f", "--output_format", dest="output_format",
            nargs='+', default=OUTPUT_FORMAT_DEAFULT.split(" "),
            choices=OUTPUT_FIELDS.split(" "), metavar="",
            help="output fields supported are: [%s] DEFAULT: [\"%s\"]" % (
                OUTPUT_FIELDS, OUTPUT_FORMAT_DEAFULT))       
        parser.add_argument("-q", "--quiet", dest="realtime",
            action="store_false",
            help="do not output results as they are recieved.")
        parser.add_argument("-m", "--multiline", dest="multiline",
            action="store_true", help="Allow output on multiple lines")
        self.args = parser.parse_args()

        self.stdin = sys_stdin.readlines()
        self.host_list = [ h.strip("\n") for h in self.stdin ]
        self.host_list = [ h for h in self.host_list if self.host_is_valid(h)]

    def host_is_valid(self, host):
        """Checks for valid hostname"""
        # Need to add in something worthwhile.
        return True

    def check_hostlist_is_valid(self):
        """Checks for a vaild hostlist, returning True or exiting"""
        if len(self.host_list) > 0:
            return True
        else:
            sys_exit("Invalid host list provided")

    def format_output(self, result, output_separator, result_format):
        """Returns a formatted string in the format specified"""
        # If we are returning the stdout_stderr result, mix them into one.
        if "stdout_stderr" in result_format:
            result["stdout_stderr"] = " ".join([ r for r in [result["stdout"],
                result["stderr"]] if r])

        return output_separator.join([str(result[f]) for f in result_format])

    def run(self):
        # Setting up the terminating event as well as the locks for writing.
        terminating = mp_Event()
        results = []

        # Create a pool of workers.
        worker_pool = mp_Pool(processes=self.args.processes,
            initializer=initializer, initargs=(terminating,))

        # Build the argument list.
        worker_args = [ (host, self.args.command, self.args.ssh_timeout, 
            self.args.realtime, self.args.multiline
            ) for host in self.host_list ]

        # Run the map, terminating on keyboard interrupt.
        try:
            for result in worker_pool.imap_unordered(ssh, worker_args):
                if self.args.realtime:
                    print self.format_output(result, self.args.output_separator,
                        self.args.output_format)
                results.append(result)
            worker_pool.close()
        except KeyboardInterrupt:
            terminating.set()
            print "Ctrl-C detected, killing processes."
            worker_pool.terminate()     
        finally:
            worker_pool.join()

        # If we are not outputting as we go, wait till the end and print.
        if not self.args.realtime:
            for result in results:
                print self.format_output(result, self.args.output_separator,
                    self.args.output_format)

if __name__ == "__main__":
    m = Main()
    m.check_hostlist_is_valid()
    m.run()
