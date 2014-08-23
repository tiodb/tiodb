import intelihubclient
import sys
import time
import argparse
import bz2
from collections import defaultdict

import time
import datetime
import pywintypes



class LogEntry(object):
  def __init__(self, line):
    self.line = line

    if line[-1] == '\n':
      line = line[:-1]
      
    fields = line.split(',', 4)

    t, command, handle, key_info, rest = fields

    size, key = self.__deserialize(key_info, rest)
    rest = rest[size+1:]
    value_info, rest = rest.split(',',1)
    size, value = self.__deserialize(value_info, rest)

    self.time_t = t
    self.command = command
    self.handle = handle
    self.key = key
    self.value = value

  def __str__(self):
    return self.__repr__()

  def __repr__(self):
    return ','.join((self.time_t, self.command, self.handle, self.key, self.value))
  

  def __deserialize(self, info, data):
    type = info[0]
    if type == 'n':
      return (0, None)

    size = int(info[1:])

    field_data = data[:size]

    ret = None

    if type == 's':
      ret = field_data
    elif type == 'i':
      ret = int(field_data)
    elif type == 'd':
      ret = float(field_data)

    return size, ret      
    
def must_ignore_this_container(name):
  return name.startswith('__')

class NullSink(object):
  def on_log_entry(self, log_entry):
    pass

class StatsLogSink(object):
  def __init__(self, speed, log_step):
    self.container_count = 0
    self.message_count = 0
    self.total_data = 0
    self.speed = speed
    self.total_changes = 0
    self.last_log = time.time()
    self.log_step = log_step
    
  def on_log_entry(self, log_entry):
    self.message_count += 1
    self.total_data += len(log_entry.line)

    if log_entry.command == 'create':
      if not must_ignore_this_container(log_entry.key):
        self.container_count += 1
    else:
      self.total_changes += 1

    log = False
    if self.speed == 0:
      if self.message_count % self.log_step == 0:
        log = True
    elif self.message_count % self.speed == 0:
      log = True

    if log:
      delta = time.time() - self.last_log
      msg_count = max(self.log_step, self.speed)
      persec = msg_count / delta
      print '%d %d containers, %d changes, %dkb so far, %0.2f msgs/s ' % \
        (self.message_count, self.container_count, self.total_changes, (self.total_data / 1024), persec)
      self.last_log = time.time()

class InteliHubLoadToMemorySink(object):
  def __init__(self):
    self.containers = {}

  def on_log_entry(self, log_entry):
    if log_entry.command == 'create':
      # we should ignore special containers
      if log_entry.key.startswith('__'):
        return False

      if log_entry.value == 'volatile_list' or log_entry.value == 'persistent_list':
        container = []
      elif log_entry.value == 'volatile_map' or log_entry.value == 'persistent_map':
        container = {}
      
      self.containers[log_entry.handle] = container
      
    else:
      container = self.containers[log_entry.handle]
      if log_entry.command == 'push_back':
        container.append(log_entry.value)
        
      elif log_entry.command == 'push_front':
        container.insert(0, log_entry.value)
        
      elif log_entry.command == 'pop_back':
        del container[-1]
        
      elif log_entry.command == 'pop_front':
        del container[0]
        
      elif log_entry.command == 'set':
        container[log_entry.key] = log_entry.value
        
      elif log_entry.command == 'insert':
        container.insert(log_entry.key, log_entry.value)
        
      elif log_entry.command == 'delete':
        del container[log_entry.key]
        
      elif log_entry.command == 'clear':
        container = type(container)()
        
      elif log_entry.command == 'propset':
        #container.propset(key, value)
        pass
        
      elif log_entry.command == 'group_add':
        #hub.group_add(key, value)
        pass
        
      else:
        raise Exception('unknown command ' + log_entry.command)

    return True


class InteliHubReplaySink(object):
  def __init__(self, hub_address, batch_size = 1):
    self.hub = intelihubclient.connect(hub_address)
    self.containers = {}
    self.batch_size = batch_size
    if self.batch_size > 1:
      self.hub.wait_for_answers = False

  def on_log_entry(self, log_entry):
    if log_entry.command == 'create':
      # we should ignore special containers
      if must_ignore_this_container(log_entry.key):
        return False
      
      container = self.hub.create(log_entry.key, log_entry.value)
      container.clear()
      self.containers[log_entry.handle] = container
    else:
      container = self.containers[log_entry.handle]
      if log_entry.command == 'push_back':
        container.push_back(log_entry.value)
      elif log_entry.command == 'push_front':
        container.push_front(log_entry.value)
      elif log_entry.command == 'pop_back':
        container.pop_back()
      elif log_entry.command == 'pop_front':
        container.pop_front()
      elif log_entry.command == 'set':
        container.set(log_entry.key, log_entry.value)
      elif log_entry.command == 'insert':
        container.insert(log_entry.key, log_entry.value)
      elif log_entry.command == 'delete':
        container.delete(log_entry.key)
      elif log_entry.command == 'clear':
        container.clear()
      elif log_entry.command == 'propset':
        container.propset(log_entry.key, log_entry.value)
      elif log_entry.command == 'group_add':
        self.hub.group_add(log_entry.key, log_entry.value)
      else:
        raise Exception('unknown command ' + command)

      if not self.hub.wait_for_answers:
        if self.hub.pending_answers_count > self.batch_size:
          self.hub.ReceivePendingAnswers()

    return True

class MultiSinkSink(object):
  def __init__(self):
    self.sinks = []

  def on_log_entry(self, log_entry):
    for sink in self.sinks:
      sink.on_log_entry(log_entry)

class InteliHubLogParser(object):
  def __init__(self, sink):
    self.speed = 10
    self.sink = sink

  def play_log_line(self, line):

    return 
    

  def wait_for_line(self, f):
    sleep_time = 0.5
    slept_time = 0
    
    while True:
      line = f.readline()

      if line:
        return line

      time.sleep(sleep_time)

      slept_time += sleep_time

      if slept_time % 5 == 0:
        print '%d seconds waiting for file to grow...' % slept_time
    
  
  def replay(self, file_path, speed, delay_seconds=0, follow=False, pause=False):
    self.speed = speed

    if file_path.endswith('bz2'):
        f = bz2.BZ2File(file_path)
    else:
        f = file(file_path, 'r')

    if pause:
      hub.server_pause()

    while 1:
      line = f.readline()

      if not line:
        if not follow:
          break
        else:
          line = self.wait_for_line(f)

      if delay_seconds > 0:
        now = int(time.time())
        message_time = int(line.split(',')[0])
        message_time += delay_seconds
        delta = message_time - now

        if delta > 0:
          if delta > 10:
            print 'sleep for %s seconds to apply a %s seconds delay' % (delta, delay_seconds)
          time.sleep(delta)

      self.sink.on_log_entry(LogEntry(line))
        
        
def main():
  argparser = argparse.ArgumentParser('Replay InteliHub logs')
  argparser.add_argument('hub')
  argparser.add_argument('file_path')
  argparser.add_argument('--speed', default=0, type=int)
  argparser.add_argument('--log-step', default=1000, type=int)
  argparser.add_argument('--delay', default=0, type=int)
  argparser.add_argument('--follow', action='store_true')
  argparser.add_argument('--pause', action='store_true', help='Pauses the server while loading. **This will disconnect all client during load time**')

  params = argparser.parse_args()

  print 'Loading file "%s" to InteliHub @ %s, %d msgs/sec, %d seconds delay %s, %s' % \
    (params.file_path, params.hub, params.speed,
     params.delay, '(follow file)' if params.follow else '', ' (pause server while loading)' if params.pause else '')

  multisink = MultiSinkSink()

  hub_sink = InteliHubReplaySink(params.hub, batch_size = params.log_step)
  multisink.sinks.append(hub_sink)

  multisink.sinks.append(StatsLogSink(params.speed, params.log_step))

  parser = InteliHubLogParser(multisink)

  if params.pause:
    hub_sink.hub.pause()

  try:
    parser.replay(
      params.file_path,
      params.speed,
      params.delay,
      params.follow)
    
  finally:
    if params.pause:
      hub.server_resume()
    

if __name__ == '__main__':
  main()

