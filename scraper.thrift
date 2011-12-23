namespace py scraper

/* Simple exception type */
exception Exception
{
    1:string msg
}

service Scraper {
    /* page url -> link href in page */
    list<string> get_links(1: string url)
    throws (1: Exception ex);

    /* page url -> img src in page */
    list<string> get_images(1: string url)
    throws (1: Exception ex);

    /* page url -> all links to depth */
    list<string> link_spider(1: string root_url, 2: i32 max_depth)
    throws (1: Exception ex);
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
