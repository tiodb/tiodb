import intelihubclient
import sys
import time
import argparse
import bz2

class InteliHubLogParser(object):
  def __init__(self):
    self.speed = 10
    pass

  def deserialize(self, info, data):
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

  def register_container(self, handle, name, type):
    self.containers

  def play_log_line(self, hub, container_dict, line):
    # remove \r
    if line[-1] == '\n':
      line = line[:-1]
      
    fields = line.split(',', 4)

    t, command, handle, key_info, rest = fields

    size, key = self.deserialize(key_info, rest)
    rest = rest[size+1:]
    value_info, rest = rest.split(',',1)
    size, value = self.deserialize(value_info, rest)

    change = False
    

    if command == 'create':
      # we should ignore special containers
      if key.startswith('__'):
        return False
      c = hub.create(key, value)
      c.clear()
      container_dict[handle] = c
    else:
      change = True
      c = container_dict[handle]
      if command == 'push_back':
        c.push_back(value)
      elif command == 'push_front':
        c.push_front(value)
      elif command == 'pop_back':
        c.pop_back()
      elif command == 'pop_front':
        c.pop_front()
      elif command == 'set':
        c.set(key, value)
      elif command == 'insert':
        c.insert(key, value)
      elif command == 'delete':
        c.delete(key)
      elif command == 'clear':
        c.clear()
      elif command == 'propset':
        c.propset(key, value)
      elif command == 'group_add':
        hub.group_add(key, value)
      else:
        raise Exception('unknown command ' + command)

    return change

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
    
  
  def replay(self, hub, file_path, speed, delay_seconds=0, follow=False, pause=False):
    containers = {}
    self.speed = speed

    if file_path.endswith('bz2'):
        f = bz2.BZ2File(file_path)
    else:
        f = file(file_path, 'r')

    count = 0
    total_data = 0
    total_changes = 0

    if pause:
      hub.server_pause()

    try:
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

        total_data += len(line)
        count += 1

        changed = self.play_log_line(hub, containers, line)

        if changed:
          total_changes += 1
                 
        log = False
        if self.speed == 0:
          if count % 1000 == 0:
            log = True
        elif count % self.speed == 0:
          log = True

        if log:
          print count, len(containers), 'containers,', total_changes, 'changes,' , (total_data / 1024), 'kb so far'
       
           
        if self.speed > 0:
            time.sleep(1.0 / self.speed)

    finally:
      if pause:
        hub.server_resume()
        
def main():
  parser = InteliHubLogParser()

  argparser = argparse.ArgumentParser('Replay InteliHub logs')
  argparser.add_argument('hub')
  argparser.add_argument('file_path')
  argparser.add_argument('--speed', default=0, type=int)
  argparser.add_argument('--delay', default=0, type=int)
  argparser.add_argument('--follow', action='store_true')
  argparser.add_argument('--pause', action='store_true', help='Pauses the server while loading. **This will disconnect all client during load time**')

  params = argparser.parse_args()

  print 'Loading file "%s" to InteliHub @ %s, %d msgs/sec, %d seconds delay %s, %s' % \
    (params.file_path, params.hub, params.speed,
     params.delay, '(follow file)' if params.follow else '', ' (pause server while loading)' if params.pause else '')
   
  hub = intelihubclient.connect(params.hub)
  
  parser.replay(
    hub,
    params.file_path,
    params.speed,
    params.delay,
    params.follow,
    params.pause)
    

if __name__ == '__main__':
  main()
    