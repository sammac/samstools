#!/usr/bin/env python

from subprocess import Popen as sp_Popen, PIPE as sp_PIPE
from threading import Thread as th_Thread

class SPWrapper(object):
    """Supply a command, then run, specifiying timeout"""
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None
        self.timed_out = None

    def run(self, timeout=None):
        """Runs subprocess in a thread with optional timeout.
        Returns (returncode,stdout,stderr,timed_out)"""
        
        def target():
            self.process = sp_Popen(self.cmd, stdout=sp_PIPE, stderr=sp_PIPE,
                shell=True)
            self.output = self.process.communicate()

        thread = th_Thread(target=target)
        thread.start()
        thread.join(timeout)
        if thread.is_alive():
            try:
                self.timed_out = True
                self.process.terminate()
                thread.join()
            except:
                pass
        else:
            self.timed_out = False
        return self.process.returncode,self.output[0],self.output[1],
        self.timed_out
