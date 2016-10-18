#!/usr/bin/python

from threading import Thread
from Queue import Queue, Empty

class StreamReader:
  def __init__(self,s):
    self.stream = s
    self.queue = Queue()

    def _populateQueue(s,q):
      while True:
        line = s.readline()
        if line:
          q.put(line)
        else:
          raise UnexpectedEndOfStream
    self.thread = Thread(target = _populateQueue, args = (self.stream, self.queue))
    self.thread.daemon = True
    self.thread.start()

  def readline(self,timeout = None):
    try:
      return self.queue.get(block = timeout is not None, timeout = timeout)
    except Empty:
      return None
   
class UnexpectedEndOfStream(Exception): pass
