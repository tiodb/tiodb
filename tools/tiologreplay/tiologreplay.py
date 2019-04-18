import sys
import os
sys.path.append(os.path.normpath(os.getcwd() + "\\.."))

import tioclient
import time
import datetime
import argparse
import bz2
from collections import defaultdict

import time
import datetime
import os.path

if sys.platform == 'win32':
    import msvcrt
    def get_key():
        if msvcrt.kbhit():
            return msvcrt.getch()
        else:
            return ''
else:
    def get_key(): return ''


class LogEntry(object):
  def __init__(self, line):
    self.line = line

    if line[-1] == '\n':
      line = line[:-1]
      
    fields = line.split(',', 4)

    t, command, handle, key_info, rest = fields

    size, key = self.__deserialize(key_info, rest)
    rest = rest[size + 1:]
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

class StatsLogger(object):
  def __init__(self):
    self.container_count = 0
    self.message_count = 0
    self.last_log_message_count = 0
    self.total_data = 0
    self.total_changes = 0
    self.last_log = time.time()
    
  def on_log_entry(self, log_entry):
    self.message_count += 1
    self.total_data += len(log_entry.line)

    if log_entry.command == 'create':
      if not must_ignore_this_container(log_entry.key):
        self.container_count += 1
    else:
      self.total_changes += 1


  def log(self):
      delta = time.time() - self.last_log
      msg_count = self.message_count - self.last_log_message_count
      persec = msg_count / delta
      print '%d %d containers, %d changes, %dkb so far, %0.2f msgs/s ' % \
        (self.message_count, self.container_count, self.total_changes, (self.total_data / 1024), persec)
      self.last_log = time.time()
      self.last_log_message_count = self.message_count

class TioLoadToMemorySink(object):
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
        #tio.group_add(key, value)
        pass
        
      else:
        raise Exception('unknown command ' + log_entry.command)

    return True


class TioReplaySink(object):
  def __init__(self, hub_address, batch_size=1, pause_server_on_load=False):
    self.tio = tioclient.connect(hub_address)

    self.pause_server_on_load = pause_server_on_load

    if self.pause_server_on_load:
        self.tio.server_pause()

    self.containers = {}
    self.batch_size = batch_size
    if self.batch_size > 1:
      self.tio.wait_for_answers = False

  def __del__(self):
    if self.pause_server_on_load:
        self.tio.server_resume()

  def on_log_entry(self, log_entry):
    if log_entry.command == 'create':
      # we should ignore special containers
      if must_ignore_this_container(log_entry.key):
        return False
      
      container = self.tio.create(log_entry.key, log_entry.value)
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
        self.tio.group_add(log_entry.key, log_entry.value)
      else:
        raise Exception('unknown command ' + command)

      if not self.tio.wait_for_answers:
        if self.tio.pending_answers_count > self.batch_size:
          self.tio.ReceivePendingAnswers()

    return True

class MultiSinkSink(object):
  def __init__(self):
    self.sinks = []

  def on_log_entry(self, log_entry):
    for sink in self.sinks:
      sink.on_log_entry(log_entry)

class TioLogResumeParser(object):
  def __init__(self, sink):
    self.speed = 10
    self.sink = sink
    self.last_read_line = 0

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
    
    file_name = os.path.basename(file_path)

    if os.path.isfile(file_name + ".dat") and os.path.getsize(file_name + ".dat"):
        with open(file_name + ".dat") as f:
            self.last_read_line = int(f.readline())
            print "Recovering from last run. We'll discard the first %s lines" % self.last_read_line
    
    last_read_file = open(file_name + ".dat", 'w')

    if file_path.endswith('bz2'):
        f = bz2.BZ2File(file_path)
    else:
        f = file(file_path, 'r')

    if pause:
      tio.server_pause()

    current_line = 0
    while 1:
        line = f.readline()
      
        if not line:
            if not follow:
                break
            else:   
                line = self.wait_for_line(f)
      
        current_line += 1
      
        if current_line <= self.last_read_line:
            continue
          
        if delay_seconds > 0:
            now = int(time.time())
            message_time = int(line.split(',')[0])
            message_time += delay_seconds
            delta = message_time - now

            if delta > 0:
                if delta > 10:
                    print 'sleep for %s seconds to apply a %s seconds delay' % (delta, delay_seconds)
                time.sleep(delta)

        try:
          self.sink.on_log_entry(LogEntry(line))
        except Exception as e:
          print 'ERROR parsing line "%s", %s ' % (line, e)

        self.last_read_line = current_line
        last_read_file.seek(0)
        last_read_file.write(str(self.last_read_line))


def change_speed_via_keyboard(current_speed):
    c = get_key()

    if not c: return current_speed        
        
    if c == '+':
        current_speed += 10
    elif c == '-' and current_speed > 1:
        current_speed -= 10
    elif c == '*':
        current_speed = 0
    elif c == '/' and current_speed > 11:
        current_speed = 1
    elif ord('9') >= ord(c) >= ord('0'):
        multiplier = int(c)
        if multiplier == 0:
            current_speed = 1
        else:
            current_speed *= multiplier

    print 'speed changed to ', current_speed
    return current_speed
        
        
class TioLogParser(object):
  def __init__(self, sink, keyboard_control=False):
    self.speed = 10
    self.sink = sink
    self.keyboard_control = keyboard_control
    self.logger = StatsLogger()

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
    
  
  def replay(self, file_path, delay_seconds, speed, follow=False, pause=False):
    self.speed = speed

    if file_path.endswith('bz2'):
        f = bz2.BZ2File(file_path)
    else:
        f = file(file_path, 'r')

    if pause:
      tio.server_pause()

    send_count = 0

    last_timestamp = time.time()

    while 1:
      # it's an option because it slows down the execution (27k/s vs 17k/s)
      if self.keyboard_control:
        speed = change_speed_via_keyboard(speed)

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

      log_line = LogEntry(line)
      self.sink.on_log_entry(log_line)
      self.logger.on_log_entry(log_line)

      send_count += 1

      if speed == 0:
          if send_count % 10000 == 0:
              self.logger.log()
      else:
         if send_count % speed == 0:
          delta = time.time() - last_timestamp
          
          if delta < 1:
            time.sleep(1 - delta)

          last_timestamp = time.time()

          self.logger.log()

        
        
def main():
  argparser = argparse.ArgumentParser('Replay Tio transaction logs')
  argparser.add_argument('tio')
  argparser.add_argument('file_path')
  argparser.add_argument('--speed', default=0, type=int, help='speed that messages will be send to tio. 0 = as fast as possible')
  argparser.add_argument('--keyboard-control', action='store_true', help='allow speed to be controlled via keyboard')
  argparser.add_argument('--log-step', default=10000, type=int, help='message interval that will be used to log progress on terminal')
  argparser.add_argument('--delay', default=0, type=int, help='Message delay relative to original message time. ' + 'We will wait if necessary to make sure the message will be replicated XX second after the time message was written to log')
  argparser.add_argument('--follow', action='store_true')
  argparser.add_argument('--pause', action='store_true', help='Pauses the server while loading. **This will disconnect all client during load time**')

  params = argparser.parse_args()

  print 'Loading file "%s" to Tio @ %s, %d msgs/sec, %s, %s' % \
    (params.file_path, params.tio, params.speed,
     '(follow file)' if params.follow else '', ' (pause server while loading)' if params.pause else '')

  parser = TioLogParser(TioReplaySink(params.tio, batch_size = params.log_step),
      keyboard_control = params.keyboard_control)

  try:
    parser.replay(params.file_path,
      params.delay,
      params.speed,
      params.delay,
      params.follow)
    
  finally:
    if params.pause:
      tio.server_resume()
    

if __name__ == '__main__':
  main()

