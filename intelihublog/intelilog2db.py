from intelihublog import MultiSinkSink,InteliHubLogParser
from win32com.client import Dispatch
from collections import defaultdict
import argparse
import datetime

class InstrumentToDbSink(object):
  def __init__(self, connection_string, insertion_interval=0):
    self.instruments = defaultdict(dict)
    self.handles = {}
    self.cn = Dispatch('ADODB.Connection')
    self.insertion_queue = []
    self.insertion_interval = insertion_interval
    self.last_insertion = datetime.datetime.now()
    print 'connecting to ', connection_string
    self.cn.Open(connection_string)

    #self.query = "INSERT INTO INTELIMARKET_PRICES (DS_ATIVO, DS_MERCADO, DT_COTACAO, VL_ATUAL, VL_ABERTURA, VL_FECHAMENTO, VL_MINIMO, VL_MEDIO, VL_MAXIMO, DT_ATUALIZACAO) " +
    #  "VALUES ('$(Symbol)s', 'VIS', TO_DATE('2014-03-24 19:33:21', 'YYYY-MM-DD HH24:MI:SS'), '%(Price)s', '19.4', '19.5', '19.2', '19.3', '19.7', TO_DATE('2014-03-25 15:33:51', 'YYYY-MM-DD HH24:MI:SS'))"


  def on_log_entry(self, log_entry):
    if log_entry.command == 'create':
      items = log_entry.key.split('/')
      if items[-1] == 'properties':
        self.handles[log_entry.handle] = items[-2]

    elif log_entry.command == 'set':
      symbol = self.handles.get(log_entry.handle)

      if symbol is None:
        return
      
      self.instruments[symbol][log_entry.key] = log_entry.value
      self.__send_symbol_to_db(symbol)

  def __send_symbol_to_db(self, symbol):
    properties = self.instruments[symbol]

    required_keys = ('SecurityType', 'Price', 'LastTradeDateTime', 'OpeningPrice', 'AveragePrice', 'MaxPrice', 'ClosingPrice')

    if not all([properties.has_key(x) for x in required_keys]):
      return

    #self.query = "INSERT INTO INTELIMARKET_PRICES (DS_ATIVO, DS_MERCADO, DT_COTACAO, VL_ATUAL, VL_ABERTURA, VL_FECHAMENTO, VL_MINIMO, VL_MEDIO, VL_MAXIMO, DT_ATUALIZACAO) " +
    #  "VALUES ('$(Symbol)s', 'VIS', TO_DATE('2014-03-24 19:33:21', 'YYYY-MM-DD HH24:MI:SS'), '%(Price)s', '19.4', '19.5', '19.2', '19.3', '19.7', TO_DATE('2014-03-25 15:33:51', 'YYYY-MM-DD HH24:MI:SS'))"

    query = '''INSERT INTO INTELIMARKET_PRICES (DS_ATIVO, DS_MERCADO, DT_COTACAO, DT_ATUALIZACAO, VL_ATUAL, VL_ABERTURA, VL_MINIMO, VL_MEDIO, VL_MAXIMO, VL_FECHAMENTO) 
      VALUES
      ('%(Symbol)s',
      '%(SecurityType)s',
      TO_DATE('%(LastTradeDateTime)s', 'YYYYMMDDHH24MISS'),
      TO_DATE('%(LastTradeDateTime)s', 'YYYYMMDDHH24MISS'),
      '%(Price)s',
      '%(OpeningPrice)s',
      '%(MinPrice)s',
      '%(AveragePrice)s',
      '%(MaxPrice)s',
      '%(ClosingPrice)s'
      )
      '''

    sql = query % properties

    print 'price for %(Symbol)s, %(LastTradeDateTime)s, %(Price)s' % properties

    if self.delayed_insertion > 0:
        delta = datetime.datetime.now() - self.last_insertion
        if delta.total_seconds() > self.insertion_interval:
            for insert in self.insertion_queue:
                self.cn.Exeucute(insert)
            self.last_insertion = datetime.datetime.now()    
        else:
            self.insertion_queue.append(sql)
    else:            
        self.cn.Execute(sql)

def main():
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

  multisink = MultiSinkSink()

  multisink.sinks.append(InstrumentToDbSink('Provider=OraOLEDB.Oracle.1;Password=123mudar;Persist Security Info=True;User ID=system;Data Source=localhost'))
  #multisink.sinks.append(InstrumentToDbSink('Provider=OraOLEDB.Oracle.1;Password=int3l1m4rk3t;Persist Security Info=True;User ID=intelimarket;Data Source=10.8.8.36'))

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

