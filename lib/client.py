from contextlib import contextmanager

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from services.discovery_service import Discovery
DISCOVERY_HOST = '127.0.0.1'
DISCOVERY_PORT = 9191
DISCOVERY_SERVICE = Discovery

@contextmanager
def connect_discovery():
    host = DISCOVERY_HOST
    port = DISCOVERY_PORT
    service = DISCOVERY_SERVICE
    transport = TSocket.TSocket(host,port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = getattr(service,'Client')(protocol)
    transport.open()
    try:
        yield client
    finally:
        transport.close()


@contextmanager
def connect(service,host=None,port=None):

    # if we didn't get a host / port use service
    # discovery to find it
    with connect_discovery() as c:
        service_name = service.__name__
        print 'discovering service: %s' % service_name
        service_details = c.find_service(service_name)
        print 'found: %s' % service_details
        assert service_details, "Could not find service in discovery"
        port = service_details.port
        host = service_details.host

    transport = TSocket.TSocket(host,port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = getattr(service,'Client')(protocol)
    transport.open()
    try:
        yield client
    finally:
        transport.close()

def test():
    with connect(Requester) as client:
        request = so.Request()
        request.url = 'http://oneinchmile.com'
        request.method = 'get'
        response = client.urlopen(request)
        #print 'response: %s' % response
    return response
