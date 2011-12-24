from requester import Requester, ttypes as o
import requests

from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport

from hashlib import sha1
from redis import Redis

class RequestHandler(object):
    def __init__(self):
        pass

    def urlopen(self, request):
        """
        returns a response for the request
        """
        raise NotImplementedError

    def cache_urlopen(self, request):
        """
        returns a response if it's in the cache
        """
        raise NotImplementedError

    def set_cache(self, request, response):
        """
        updates the cache w/ the given response
        """
        raise NotImplementedError

    def live_urlopen(self, request):
        """
        makes request to host and returns response
        """
        raise NotImplementedError

    def check_rate_allowed(self, request):
        """
        returns the rate you are allowed to pull data from
        the host at
        """
        raise NotImplementedError

class LiveRequestHandler(RequestHandler):

    timeout = 5

    def urlopen(self,request):
        return self.live_urlopen(request)

    def live_urlopen(self, request):

        try:
            # use the requests getter to get the resource
            http_response = requests.request(request.method,
                                             request.url,
                                             cookies=request.cookies,
                                             timeout=self.timeout,
                                             # don't just get headers
                                             prefetch=True,
                                             # we want raw data, not unicode
                                             config={'decode_unicode':False})
        except Exception, ex:
            # problem actually trying to get the resource
            raise o.Exception('HTTP Request Error: %s' % ex)

        # build our response obj / aka copy that shit
        response = o.Response()
        response.status_code = http_response.status_code
        response.url = http_response.url
        response.headers = http_response.headers
        response.content = http_response.content

        # and we're done!
        return response

class CachingRequestHandler(RequestHandler):
    def __init__(self,redis_host='127.0.0.1',cache_duration=2*60*60):
        self.redis_host = redis_host
        self.cache_duration = cache_duration
        self.rc = Redis(self.redis_host)
        self.pfactory = TBinaryProtocol.TBinaryProtocolFactory()
        super(CachingRequestHandler,self).__init__()

    def urlopen(self,request):
        return self.cached_urlopen(request)

    def cache_urlopen(self,request):
        # check the cache
        cache_key = self.get_cache_key(request)
        cache_response = self.rc.get(cache_key)

        # no cache hit?
        if not cache_response:
            return None # TODO: see if i can even do this

        # deserialize our response
        response = self._deserialize_o(o.Response,cache_response)
        return response

    def set_cache(self,request,response):
        """ caches the response for a request """
        url = request.url
        cache_key = self.get_cache_key(request)
        data = self._serialize_o(response)
        self.rc.set(cache_key,data)
        self.rc.expire(cache_key,self.cache_duration)
        return True

    def get_cache_key(self,request):
        """ returns the key for the given request in the cache """
        return 'httpcache:%s' % sha1(request.url).hexdigest()

    def _serialize_o(self, obj):
        trans = TTransport.TMemoryBuffer()
        prot = self.pfactory.getProtocol(trans)
        obj.write(prot)
        return trans.getvalue()

    def _deserialize_o(self, objtype, data):
        print 'deserialize: %s' % len(data)
        prot = self.pfactory.getProtocol(TTransport.TMemoryBuffer(data))
        ret = objtype()
        ret.read(prot)
        return ret

class MatureRequestHandler(CachingRequestHandler,LiveRequestHandler):

    def urlopen(self, request):
        print 'urlopen'
        # check the cache
        response = self.cache_urlopen(request)
        if response:
            print 'response from cache'

        # check and make sure we aren't going to have
        # to fail due to rate limiting for the site
        # this should return the rate at which we can
        # pull data. for now a non 0 is an OK
        if not response:
            allowed_rate = self.check_rate_allowed(request)
            print 'allow rate: %s' % allowed_rate

        # make our request
        if not response and allowed_rate:
            response = self.live_urlopen(request)
            print 'live response'
            # update the cache
            self.set_cache(request,response)

        # return the response
        return response

    def check_rate_allowed(self, request):
        return 1

    def warm_cache(self, request):
        """
        pulls a page but doesn't return it, just cache's it
        """
        print 'warm cache'

        # this will return me a response but i ignore it
        self.urlopen(request)

def run():
    from run_services import serve_service
    serve_service(Requester, MatureRequestHandler())

if __name__ == '__main__':
    run()
