//include "../lib/requester/tdefs/requester.thrift"
include "requester.thrift"

namespace py scraper

/* Simple exception type */
exception Exception
{
    1:string msg
}


struct Page {
    1: string url,
    2: optional requester.Response response,
    3: optional list<string> links,
    4: optional list<string> images,
}

struct SpiderResponse {
    1: string url,
    2: optional list<Page> pages
}

service Scraper {
    /* page url -> link href in page */
    list<string> get_links(1: string url)
    throws (1: Exception ex);

    /* page url -> img src in page */
    list<string> get_images(1: string url)
    throws (1: Exception ex);

    /* page url -> all links to depth */
    list<string> link_spider(1: string root_url, 
                             2: i32 max_depth, 
                             3: bool off_root,
                             4: string prefix)
    throws (1: Exception ex);

    /* page url -> details about site pages */
    SpiderResponse site_spider(1: string root_url)
    throws (1: Exception ex);

    /* site root -> xml site map */
    string gen_sitemap(1: string root_url)
    throws (1: Exception ex);
}


