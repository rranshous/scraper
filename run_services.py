
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

def serve(host='127.0.0.1',port=9191):
    # setup the processors for each of the services
    from scraper import Scraper, Requester

    # the requests handler
    from request_service import MatureRequestHandler
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

if __name__ == '__main__':
    print 'starting serviing'
    serve()
