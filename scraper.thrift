namespace py scraper

/* Simple exception type */
exception Exception
{
    1:string msg
}

struct Request {
    1: string method,
    2: string url,
    3: map<string,string> data,
    4: map<string,string> cookies
}

struct Response {
    1: i32 status_code,
    2: string url,
    3: map<string,string> headers,
    4: string content
}

service Requester {
    /* does http request for resorce */
    Response urlopen(1: Request request)
    throws (1: Exception ex)
}

service Scraper {
    /* page url -> link href in page */
    list<string> get_links(1: string url)
    throws (1: Exception ex)

    /* page url -> img src in page */
    list<string> get_images(1: string url)
    throws (1: Exception ex)

    /* page url -> all links to depth */
    list<string> link_spider(1: string root_url, 2: i32 max_depth)
    throws (1: Exception ex)
}


/*
service TumblrImageScraper {
    validate_page_url
    get_page_urls
    get_pic_urls
    save_pic
    generate_pic_details
}
*/
