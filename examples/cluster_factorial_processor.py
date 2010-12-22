import tioclient
tio = tioclient.Connect('tio://127.0.0.1:12345')
source = tio.CreateContainer('factorial_cluster/source', 'volatile_list')

destination_name = source.get_property('destination')
destination = tio.OpenContainer(destination_name)

def factorial(n):
	if n == 0: return 1
	return n * factorial(n -1)

def GottaWorkToDo(container, event_name, key, value, metadata):
	print 'calculating factorial(%d)' % value
	# like "'20!=2432902008176640000'"
	destination.push_back('%d!=%s' % (value, factorial(value)))
	source.wait_and_pop_next(GottaWorkToDo)

source.wait_and_pop_next(GottaWorkToDo)
tio.RunLoop()