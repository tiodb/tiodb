import intelihubclient
import sys
import time
from optparse import OptionParser
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
    
  
  def replay(self, hub, file_path):
    containers = {}

    if file_path.endswith('bz2'):
        f = bz2.BZ2File(file_path)
    else:
        f = file(file_path, 'r')

    count = 0
    total_data = 0
    total_changes = 0

    while 1:
      line = f.readline()

      if not line:
        break

      total_data += len(line)
      count += 1
               
      log = False
      if self.speed == 0:
        if count % 1000 == 0:
          log = True
      elif count % self.speed == 0:
        log = True

      if log:
        print count, len(containers), 'containers,', total_changes, 'changes,' , (total_data / 1024), 'kb so far'
      

      # remove \r
      line = line[:-1]
      fields = line.split(',', 4)

      t, command, handle, key_info, rest = fields

      size, key = self.deserialize(key_info, rest)
      rest = rest[size+1:]
      value_info, rest = rest.split(',',1)
      size, value = self.deserialize(value_info, rest)
      

      if command == 'create':
        c = hub.create(key, value)
        c.clear()
        containers[handle] = c
      else:
        total_changes += 1
        c = containers[handle]
        if command == 'push_back':
          c.push_back(value)
        elif command == 'push_front':
          c.push_front(value)
        if command == 'pop_back':
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
        else:
          raise Exception('unknown command ' + command)
          
        if self.speed > 0:
            time.sleep(1.0 / self.speed)
        
def main():
  parser = InteliHubLogParser()
  hub_host, file_path, speed = sys.argv[1:]
  speed = int(speed)

  print 'Loading file "%s" to InteliHub @ %s, %d msgs/sec' % (file_path, hub_host, speed)
  
  parser.speed = speed
  
  hub = intelihubclient.connect(hub_host)
  
  parser.replay(hub, file_path)

if __name__ == '__main__':
  main()
    