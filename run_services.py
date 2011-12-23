
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

def serve(host='127.0.0.1',port=9191):
    # setup the processors for each of the services
    from services.scraper import Scraper
    from services.requester import Requester

    # the requests handler
    from services.request_service import MatureRequestHandler
    handler = MatureRequestHandler()
    processor = Requester.Processor(handler)

    # the scraper handler
    from scraper_service import ScraperHandler
    handler = ScraperHandler()
    processor = Scraper.Processor(handler)

    transport = TSocket.TServerSocket(host,port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

    print 'starting'
    server.serve()
    print 'done'


def serve_service(service, handler, host='127.0.0.1',port=9191):
    processor = getattr(service,'Processor')(handler)
    transport = TSocket.TServerSocket(host,port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    print 'starting: %s %s %s' % (service,host,port)
    server.serve()
    print 'done'

if __name__ == '__main__':
    import sys
    n = sys.argv[1]
    print 'starting: %s' % n
    mn = 'services.%s' % n
    print 'module name: %s' % mn
    import_string = 'from %s import run as run_service' % mn
    exec(import_string)
    print 'running'
    run_service()
