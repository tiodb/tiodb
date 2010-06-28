import sys
import tioclient
import cgi
import os
import time
import traceback
import json
import functools
from cStringIO import StringIO
        
class CgiResponse(object):
    def __init__(self):
        self.stream = sys.stdout
        pass

    def send_http_headers(self):
        print "Content-Type: text/html\n\n"

    def write(self, what):
        print what

class TioWeb(object):
    def __init__(self, tio, container_prefix = 'tioweb/publications/'):
        self.tio = tio
        self.container_prefix = container_prefix

        self.containers_by_handle = {}
        self.containers_by_name = {}

        self.debug = False

        self.__load_dispatch_map()        

    def __new_session_id(self):
        self.session_id = str(time.time()).replace('.', '')
        container_name = self.container_prefix + self.session_id
        self.publications_container = self.create_container(container_name, 'volatile/list')

    def __set_session_id(self, session_id):
        self.session_id = session_id
        container_name = self.container_prefix + session_id
        self.publications_container = self.open_container(container_name)
        
    def __add_container(self, container):
        self.containers_by_handle[container.handle] = container
        self.containers_by_name[container.name] = container.handle
        return container

    def __get_container(self, name_or_handle):
        try:
            return self.containers_by_handle[name_or_handle]
        except:
            pass

        return self.containers_by_handle[self.containers_by_name[name_or_handle]]

    def create_container(self, name, type):
        return self.__add_container(self.tio.CreateContainer(name, type))

    def open_container(self, name, type=None):
        return self.__add_container(self.tio.OpenContainer(name, type))
    
    def dispatch(self, form):
        self.form = form

        if form.has_key('debug') and form['debug'].value.lower() in ('y', 'yes', '1', 'on', 'true'):
            self.debug = True        

        if not form.has_key('command'):
            raise Exception('command?')
            return

        if form.has_key('session_id'):
            self.__set_session_id(form['session_id'].value)

        command = form['command'].value

        if not command in self.dispatch_map:
            raise Exception('invalid command: %s' % command)
            
        return self.dispatch_map[command]()

    #
    #
    # commands
    #
    #
    def __load_dispatch_map(self):
        self.dispatch_map = {}

        self.dispatch_map['subscribe'] = self.subscribe
        self.dispatch_map['new_session'] = self.new_session
        self.dispatch_map['open'] = self.open
        self.dispatch_map['create'] = self.create
        self.dispatch_map['get_count'] = self.get_count
        self.dispatch_map['publications'] = self.publications
        self.dispatch_map['query'] = self.query

        def add_data_command(name):
            self.dispatch_map[name] = functools.partial(self.__send_data_command, name)

        add_data_command('push_back')
        add_data_command('push_front')
        add_data_command('insert')
        add_data_command('set')
        add_data_command('get')
        add_data_command('pop_front')
        add_data_command('pop_back')
        add_data_command('delete')

    def query(self):
        container = self.open_container(self.form['container'].value)

        start = int(self.form['start'].value) if 'start' in self.form else 0
        end = int(self.form['end'].value) if 'end' in self.form else 0

        query_limit = 10 * 1000
        
        if end == 0 or end - start > query_limit:
            end = start + query_limit
            query_type = 'maybe_truncated'
        else:
            query_type = 'full'
                    
        ret = container.query_with_key_and_metadata(start, end)

        if container.type in ('volatile_map', 'persistent_map'):
            result_set = {}
            for key, value, metadata in ret:
                result_set[key] = {'key': key, 'value': value, 'metadata': metadata }
        else:
            result_set = [dict(zip(('key', 'value', 'metadata'), x)) for x in ret]
        
        return {'result': 'ok', 'query_type': query_type, 'result_set': result_set}        
    
    def subscribe(self):
        container = self.open_container(self.form['container'].value)
        container.start_recording(self.publications_container)
        return {'result': 'ok'}

    def open(self):
        container = self.open_container(self.form['container'].value)
        return {'result': 'ok'}
    
    def create(self):
        container = self.create_container(self.form['container'].value, self.form['type'].value)
        return {'result': 'ok'}

    def publications(self):
        ret = {'result': 'ok'}
        pubs = []

        count = len(self.publications_container)

        if self.form.has_key('limit'):
            limit = int(self.form['limit'].value)
            if limit < count:
                count = limit

        for index in range(count):
            key, value, metadata = self.publications_container.get(index, withKeyAndMetadata=True)
            pub_values = {'container' : metadata}

            # the event info in codified in the "value" field
            event, key, value, metadata = tioclient.decode(value)
            pub_values['event'] = event
            if not key is None: pub_values['key'] = key
            if not value is None: pub_values['value'] = value
            if not metadata is None: pub_values['metadata'] = metadata

            pubs.append(pub_values)

        # this way we'll remove items from the list only if everything is ok
        for index in range(count):
            self.publications_container.pop_front()

        ret['publications'] = pubs

        return ret        
                

    def __send_data_command(self, command):
        container = self.open_container(self.form['container'].value)

        def get_field(name):
            if not self.form.has_key(name):
                return None

            value = self.form[name].value

            type_field = name + '_type'
            
            if self.form.has_key(type_field):
                type = self.form[type_field].value
                if type == 'int':
                    value = int(value)
                elif type == 'double':
                    value = float(value)
                else:
                    raise Exception('invalid data type: %s' % type)

            return value                    
            
        key = get_field('key')
        value = get_field('value')
        metadata = get_field('metadata')
        
        ret = container.send_data_command(command, key, value, metadata)

        if isinstance(ret, tuple):
            key, value, metadata = ret

        ret = {'result': 'ok'}

        if key: ret['key'] = key
        if value: ret['value'] = value
        if metadata: ret['metadata'] = metadata

        return ret        

    def get_count(self):
        container = self.open_container(self.form['container'].value)
        return {'result': 'ok', 'count': len(container) }
    
    def new_session(self):
        self.__new_session_id()
        return {'result': 'ok', 'session_id': self.session_id}

         
def doit(tio, form):    
    try:        
        tioweb = TioWeb(tio)

        ret = []

        result = tioweb.dispatch(form)

        if not tioweb.debug:
            ret.append(json.dumps(result, separators=(',',':')))
        else:
            encoded = json.dumps(result, indent=2)
            ret.append('<script language="javascript">var ret = %s;</script>' % encoded)

        if tioweb.debug:
            ret.append('<pre>%s</pre>' % cgi.escape(result))
                    
    except Exception, ex:
        debug = True
        try:
            debug = tioweb.debug
        except:
            pass
        
        if debug:
            ret.append('<pre>')
            ret.append('Exception: %s\r\n' % ex)
            #traceback.print_tb(sys.exc_info()[2], limit=None, file=response.stream)
            ret.append('</pre>')

        ret.append(json.dumps({'result': 'error', 'description': str(ex)}))

        if debug:
            ret.append('<pre>%s</pre>' % cgi.escape(ret))        

    return ret

tio_connection = None

def application(environ, start_response):
    headers = []
    headers.append(('Content-Type', 'text/html'))
    write = start_response('200 OK', headers)

    global tio_connection

    if not tio_connection:
        tio_connection = tioclient.Connect(environ['tio.server']) 

    form = cgi.FieldStorage(fp=environ['wsgi.input'], environ=environ, keep_blank_values=True)

    ret = doit(tio_connection, form)
    #ret.extend(dump_environ(environ))

    return ret

def dump_environ(environ):
    input = environ['wsgi.input']
    output = StringIO()

    print >> output, "PID: %s" % os.getpid()
    print >> output, "UID: %s" % os.getuid()
    print >> output, "GID: %s" % os.getgid()
    print >> output

    keys = environ.keys()
    keys.sort()
    for key in keys:
        print >> output, '%s: %s' % (key, repr(environ[key]))
        print >> output

        output.write(input.read(int(environ.get('CONTENT_LENGTH', '0'))))

    return ['<pre>', output.getvalue(), '</pre>']


def main():
    from wsgiref.simple_server import make_server
    print 'running server on localhost:8080'
    srv = make_server('localhost', 8080, application)
    srv.base_environ['tio.server'] = 'tio://127.0.0.1:6666'
    srv.serve_forever()

if __name__ == '__main__':
    main()