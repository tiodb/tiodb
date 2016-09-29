import random
import tioclient
tio = tioclient.Connect('tio://127.0.0.1:12345')
source = tio.CreateContainer('factorial_cluster/source', 'volatile_list')
destination = tio.CreateContainer('factorial_cluster/destination', 'volatile_list')

source.set_property('destination', 'factorial_cluster/destination')

def OnItemDone(container, event_name, key, value, metadata):
    print value
    
destination.subscribe(OnItemDone)

# add 10k integers from 1 to 500
item_count = 10 * 1000
for x in range(item_count):
    source.append(random.randint(1,500))
    
print 'generated %d items, now waiting for results' % item_count

tio.RunLoop()

	