from requester import Requester, ttypes as ro
from scraper import Scraper, ttypes as o

import requests
from hashlib import sha1
from redis import Redis
from BeautifulSoup import BeautifulSoup as BS
from urlparse import urljoin
from cStringIO import StringIO
import imghdr
from urlparse import urlparse
from lib.helpers import fixurl

from thread_utils import thread_out_work

from lib.client import connect as srvs_connect


class ScraperHandler(object):

    not_html_ext = ('jpg','jpeg','pdf','gif','png')

    def __init__(self,redis_host='127.0.0.1'):
        self.redis_host = redis_host
        self.rc = Redis(self.redis_host)

    @staticmethod
    def _is_img_data(data):
        """
        tests whether the passed data is that of an image, returns bool
        """
        # create a io buffer
        b = StringIO(data)

        # get the image type
        img_type = imghdr.what(b)
        b.close()

        # return true if it's a known type
        if img_type:
            return True

        return False

    def get_links(self, url):
        """ returns back the href for all links on page """

        url = url.strip()
        print 'get_links: %s' % url

        # if it's an image forget it
        if url.lower().endswith(self.not_html_ext):
            return []

        # request the url
        try:
            with srvs_connect(Requester) as c:
                r = c.urlopen(ro.Request(url))
            if not r:
                return []
        except o.Exception, ex:
            raise o.Exception('o.Could not make request: %s %s' % (url,ex))
        except Exception, ex:
            raise o.Exception('Could not make request: %s %s' % (url,ex))

        # see if we have a cache of links
        digest = sha1(r.content).hexdigest()
        cache_key = 'scraper:get_links:content:%s' % digest
        cache_response = self.rc.smembers(cache_key)
        if cache_response:
            print 'from cache'
            return cache_response

        # if it's an image than we know the answer
        try:
            if self._is_img_data(r.content):
                return []
        except:
            # we'll see
            pass

        # get all the links
        try:
            print 'parsing'
            soup = BS(r.content)
        except o.Exception, ex:
            raise o.Exception('o.Could not parse response: %s %s' % (url,ex))
        except Exception, ex:
            raise o.Exception('Could not parse response: %s %s' % (url,ex))

        links = []
        for link in soup.findAll('a'):
            if link.get('href'):
                link_href = urljoin(url,link.get('href'))
                links.append(link_href)

        # set our cache, no need to expire b/c it's based on the content
        if links:
            self.rc.sadd(cache_key,*list(links))

        return links

    def get_images(self, url):
        """ returns back the src for all images on page """

        url = url.strip()
        print 'get_images: %s' % url

        # only care to parse html pages
        if url.lower().endswith(self.not_html_ext):
            return []

        # request the url
        try:
            with srvs_connect(Requester) as c:
                r = c.urlopen(ro.Request(url))
            if not r:
                return []
        except o.Exception, ex:
            raise o.Exception('o.Could not make request: %s %s' % (url,ex))
        except Exception, ex:
            raise o.Exception('Could not make request: %s %s' % (url,ex))

        # see if we have a cache of links
        digest = sha1(r.content).hexdigest()
        cache_key = 'scraper:get_images:content:%s' % digest
        cache_response = self.rc.smembers(cache_key)
        if cache_response:
            print 'from cached'
            return cache_response

        # if it's an image than we know the answer
        try:
            if self._is_img_data(r.content):
                return []
        except:
            # we'll see
            pass

        # get all the images
        soup = BS(r.content)
        images = []
        for img in soup.findAll('img'):
            if img.get('src'):
                images.append(img.get('src'))

        # set our cache, no need to expire b/c it's based on the content
        if images:
            self.rc.sadd(cache_key,images)

        return images


    def _clean_links(self, links):
        # we only want http(s) (other being mailto, javascript etc)
        links = [x.strip() for x in links if x.startswith('http')]
        return links

    def _off_root(self,root,link):
        """ returns true if links is from same domain """
        # TODO: cache root url netloc
        return urlparse(root).netloc == urlparse(link).netloc

    def _clean_off_root(self, root, links):
        """ returns list filtered for those off root """
        return [l for l in links if self._off_root(root,l)]

    def link_spider(self, root_url, max_depth, limit_domain=False):
        """ returns back all the links to given depth """

        print 'link spider: %s %s' % (root_url,max_depth)

        # starting at the root url follow all links
        # keep following those links until they exceed the depth

        try:
            # our first link is root
            links = set([(root_url,0)])

            # initial links
            found_links = set()

            while links:

                # get the next url
                link, depth = links.pop()

                # where does it link to ?
                result_links = []
                try:
                    result_links = self.get_links(link)
                except o.Exception, ex:
                    print 'oException getting link: %s %s' % (link,ex.msg)
                except Exception, ex:
                    print 'Exception getting link: %s %s' % (link,ex)

                # clean up the result
                result_links = self._clean_links(result_links)

                # if we are limiting the domain, we only want
                # links which are part of the root url
                if limit_domain:
                    result_links = self._clean_off_root(root_url,result_links)

                result_links = set(result_links)

                if result_links:
                    print 'result links: %s' % len(result_links)

                # if the next lvl isn't max depth, add in new links
                if depth + 1 <= max_depth:
                    # only add links we haven't seen before
                    ts = result_links - found_links

                    # add in our depth
                    ts = [(l,depth+1) for l in ts]

                    links.update(set(ts))

                found_links.update(result_links)

                print 'workable links: %s' % len(links)

        except o.Exception, ex:
            # fail, ignore for now
            raise o.Exception('oException getting links: %s' % (ex.msg))
        except Exception, ex:
            # fail, ignore for now
            raise o.Exception('Exception getting links: %s' % (ex))

        # return our bounty
        return list(found_links)

    def site_spider(self,root_url):
        """
        spider every page of the site we can find, report back
        with links found and their details
        """

        response = o.SpiderResponse(url=root_url)
        response.pages = []

        # starting @ the root spider all the sites we can find w/in
        # the domain
        links = self.link_spider(root_url,100,True)

        # all that data is nice and cached so we can reprocess it
        for link in links + [root_url]:
            page = o.Page(url=link)
            page.links = self.get_links(link)
            page.images = self.get_images(link)
            try:
                with srvs_connect(Requester) as c:
                    r = c.urlopen(ro.Request(link))
                page.response = r
            except o.Exception, ex:
                # problem w/ response = no response
                print 'o.request exception: %s %s' % (link,ex.msg)
            except Exception, ex:
                print 'request exception: %s %s' % (link,ex)
            response.pages.append(page)

        return response


def run():
    from run_services import serve_service
    serve_service(Scraper, ScraperHandler())

if __name__ == '__main__':
    run()
