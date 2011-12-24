from requester import Requester, ttypes as ro
from scraper import Scraper, ttypes as o

import requests
from hashlib import sha1
from redis import Redis
from BeautifulSoup import BeautifulSoup as BS
from urlparse import urljoin
from cStringIO import StringIO
import imghdr

from lib.client import connect as srvs_connect

class ScraperHandler(object):

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

        print 'get_links: %s' % url

        # if it's an image forget it
        if url.endswith(('JPG','jpg','JPEG','jpeg','png','PNG')):
            return []

        # request the url
        try:
            with srvs_connect(Requester) as c:
                r = c.urlopen(ro.Request(method='get',url=url.strip()))
            if not r:
                return []
        except o.Exception, ex:
            raise o.Exception('o.Could not make request: %s %s' % (url,ex))
        except Exception, ex:
            raise o.Exception('Could not make request: %s %s' % (url,ex))

        # if it's an image than we know the answer
        try:
            if self._is_img_data(r.content):
                return []
        except:
            # we'll see
            pass

        # get all the links
        try:
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

        return links

    def get_images(self, url):
        """ returns back the src for all images on page """

        # if it's an image forget it
        if url.endswith(('JPG','jpg','JPEG','jpeg','png','PNG')):
            return []

        # request the url
        try:
            with srvs_connect(Requester) as c:
                r = c.urlopen(ro.Request(method='get',url=url.strip()))
            if not r:
                return []
        except Exception, ex:
            raise o.Exception('Could not make request: %s %s' % (url,ex))

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

        return images

    def link_spider(self, root_url, max_depth):
        """ returns back all the links to given depth """

        # starting at the root url follow all links
        # keep following those links until they exceed the depth

        # list of urls to scrape, (url,depth)
        found_links = self.get_links(root_url)
        links = set([(x,0) for x in found_links])
        while links:
            # next ?
            page_url, depth = links.pop()

            # make sure we're not in too deep
            if not depth + 1 > max_depth:
                try:
                    new_links = [x.strip() for x in self.get_links(page_url)]
                    # we only want http(s) (other being mailto, javascript etc)
                    new_links = [x for x in new_links if x.startswith('http')]
                    found_links += new_links
                    new_links = set([(x,depth+1) for x in new_links if x.startswith('http')])
                    print 'new links: %s' % len(new_links)
                    links.update(new_links)
                except o.Exception, ex:
                    # fail, ignore for now
                    print 'o.Exception getting links: %s %s' % (page_url,ex.msg)
                except Exception, ex:
                    # fail, ignore for now
                    print 'Exception getting links: %s %s' % (page_url,ex)

        # return our bounty
        return list(set(found_links))

def run():
    from run_services import serve_service
    serve_service(Scraper, ScraperHandler())

if __name__ == '__main__':
    run()
