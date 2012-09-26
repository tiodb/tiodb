import intelihubclient
import sys


class InteliHubLogParser(object):
  def __init__(self):
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
    f = file(file_path, 'r')

    count = 0

    while 1:
      line = f.readline()

      if not line:
        break

      count += 1
      if count % 1000 == 0:
        print count, len(containers), 'containers'

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
        containers[handle] = c
      else:
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
        
def main():
  parser = InteliHubLogParser()
  hub_host, file_path = sys.argv[1:]

  print 'Loading file "', file_path, '" to InteliHub @', hub_host
  
  hub = intelihubclient.connect(hub_host)
  
  parser.replay(hub, file_path)

if __name__ == '__main__':
  main()
    