from lib.requester import Requester, o as ro
from tgen.scraper import Scraper, ttypes as o

import requests
import memcache
from xml.sax.saxutils import escape
from hashlib import sha1
from redis import Redis
from BeautifulSoup import BeautifulSoup as BS
from urlparse import urljoin
from cStringIO import StringIO
import imghdr
from urlparse import urlparse
from lib.helpers import fixurl
from lib.httpheader import parse_http_datetime

from thread_utils import thread_out_work

from lib.discovery import connect as srvs_connect

class ScraperHandler(object):

    not_html_ext = ('jpg','jpeg','pdf','gif','png')

    def __init__(self, memcached_host='127.0.0.1', memcached_port=11211):
        self.memcached_host = memcached_host
        self.memcached_port = memcached_port
        self.mc = memcache.Client(['%s:%s' %
                            (self.memcached_host,self.memcached_port)])

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

        # if it's an image than we know the answer
        try:
            if self._is_img_data(r.content):
                return []
        except:
            # we'll see
            pass

        # see if we have a cache of links
        digest = sha1(r.content).hexdigest()
        cache_key = 'scraper:get_links:content:%s' % digest
        cache_response = self.mc.get(cache_key)
        if cache_response:
            # we r storing as new line seperated urls
            cache_response = cache_response.split('\n')
            print 'from cache'
            return cache_response

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
            # store as newline seperated values
            self.mc.set(cache_key,'\n'.join(links))

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
            print 'get image making request: %s' % url
            with srvs_connect(Requester) as c:
                r = c.urlopen(ro.Request(url))
            if not r:
                print 'get image no response: %s' % url
                return []
        except o.Exception, ex:
            print 'ex'
            raise o.Exception('o.Could not make request: %s %s' % (url,ex))
        except Exception, ex:
            print 'ex'
            raise o.Exception('Could not make request: %s %s' % (url,ex))

        if not r.content:
            print 'no content: %s' % url
            return []

        # see if we have a cache of links
        digest = sha1(r.content).hexdigest()
        cache_key = 'scraper:get_images:content:%s' % digest
        cache_response = self.mc.get(cache_key)
        if cache_response:
            # we r storing as new line seperated urls
            cache_response = cache_response.split('\n')
            print 'from cached: %s' % url
            return cache_response

        # if it's an image than we know the answer
        try:
            if self._is_img_data(r.content):
                print 'not html: %s' % url
                return []
        except:
            # we'll see
            pass

        # get all the images
        soup = BS(r.content)
        images = set()
        for img in soup.findAll('img'):
            if img.get('src'):
                img_src = urljoin(url,img.get('src'))
                images.add(img_src)
        images = list(images)

        # set our cache, no need to expire b/c it's based on the content
        if images:
            self.mc.set(cache_key,'\n'.join(images))

        print 'returning get images: %s' % url

        return images


    def _clean_links(self, links):
        # we only want http(s) (other being mailto, javascript etc)
        links = [x.strip() for x in links if x.startswith('http')]
        return links

    def _off_root(self,root,link):
        """ returns true if links is from same domain """
        # TODO: cache root url netloc
        return urlparse(root).netloc == urlparse(link).netloc

    def _off_prefix(self,prefix,link):
        if not prefix.startswith('/'):
            prefix = '/%s' % prefix
        return urlparse(link).path.startswith(prefix)

    def _clean_off_root(self, root, links):
        """ returns list filtered for those off root """
        return [l for l in links if self._off_root(root,l)]

    def _clean_off_prefix(self, prefix, links):
        return [l for l in links if self._off_prefix(prefix,l)]

    def link_spider(self, root_url, max_depth, limit_domain=False,
                    prefix=None):
        """ returns back all the links to given depth """

        print 'link spider: %s %s' % (root_url,max_depth)

        # starting at the root url follow all links
        # keep following those links until they exceed the depth

        have_failed = set()

        try:
            # our first link is root
            links = set([(root_url,0)])

            # initial links
            found_links = set()

            while links:

                # get the next url
                link, depth = links.pop()

                # where does it link to ?
                try:
                    result_links = self.get_links(link)
                except o.Exception, ex:
                    print 'oException getting link: %s %s' % (link,ex.msg)
                    if not link in have_failed:
                        print 'retrying'
                        links.add((link,depth))
                        have_failed.add(link)
                    continue
                except Exception, ex:
                    print 'Exception getting link: %s %s' % (link,ex)
                    if not link in have_failed:
                        print 'retrying'
                        links.add((link,depth))
                        have_failed.add(link)
                    continue

                # clean up the result
                result_links = self._clean_links(result_links)

                # if we are limiting the domain, we only want
                # links which are part of the root url
                if limit_domain:
                    result_links = self._clean_off_root(root_url,result_links)

                if prefix:
                    result_links = self._clean_off_prefix(prefix,result_links)

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
        links = self.link_spider(root_url,1000,True)

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

    def gen_sitemap(self, root_url):
        """
        generates an xml sitemap for given site via spider

        ex from sitemap.org)

        <?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
        <url>
        <loc>http://www.example.com/</loc>
        <lastmod>2005-01-01</lastmod>
        <changefreq>monthly</changefreq>
        <priority>0.8</priority>
        </url>
        </urlset>
        """

        # spider the site
        print 'gen sitemap: %s' % root_url
        spider_response = self.site_spider(root_url)

        output = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">"""

        print 'creating entries'

        # go through all the pages, creating entries
        entry_template = """
<url>
    <loc>%(url)s</loc>
    <lastmod>%(lastmod)s</lastmod>
</url>
"""
        for page in spider_response.pages:
            args = { 'url':escape(page.url),
                     'lastmod':'' }
            # simple for now
            if page.response.headers.get('last-modified'):
                lm = page.response.headers.get('last-modified')
                lm = parse_http_datetime(lm)
                lm = lm.strftime('%Y-%m-%d')
                args['lastmod'] = escape(lm)
            output += entry_template % args

        output += "\n</urlset>"

        print 'done: %s' % len(output)

        return output


def run():
    from lib.discovery import serve_service
    serve_service(Scraper, ScraperHandler())

if __name__ == '__main__':
    run()
