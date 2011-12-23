from scraper import Scraper, Requester, ttypes as o
import requests

from hashlib import sha1
from redis import Redis

from client import connect as srvs_connect


class ScraperHandler(object):

    def get_links(self, url):
        """ returns back the href for all links on page """

        # request the url
        with srvs_connect(Requester) as c:
            r = c.urlopen(o.Request(method='get',url=url))
        if not r:
            return []

        # get all the links
        soup = BS(r.content)
        links = []
        for link in soup.findAll('a'):
            links.append(link.get('href'))

        return links

    def get_images(self, url):
        """ returns back the src for all images on page """

        # request the url
        with srvs_connect(Requester) as c:
            r = c.urlopen(o.Request(method='get',url=url))
        if not r:
            return []

        # get all the images
        soup = BS(r.content)
        images = []
        for img in soup.findAll('img'):
            images.append(img.get('src'))

        return images

    def link_spider(self, root_url, max_depth):
        """ returns back all the links to given depth """

        # starting at the root url follow all links
        # keep following those links until they exceed the depth

        # list of urls to scrape, (url,depth)
        links = [(x,0) for x in self.get_links(root_url)]
        while links:
            # next ?
            page_url, depth = links.pop()

            # make sure we're not in too deep
            if not depth + 1 > max_depth:
                links += [(x,depth+1) for x in self.get_links(page_url)]

        # return our bounty
        return links
