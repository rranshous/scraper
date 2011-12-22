from scraper import Scraper, ttypes as o
from contextlib import contextmanager

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

@contextmanager
def connect(host='127.0.0.1',port=9191):
    transport = TSocket.TSocket(host,port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Scraper.Client(protocol)
    transport.open()
    yield client
    transport.close()

def test():
    with connect() as client:
        request = o.Request()
        request.url = 'http://oneinchmile.com'
        request.method = 'get'
        response = client.urlopen(request)
        #print 'response: %s' % response
    return response
