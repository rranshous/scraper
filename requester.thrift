namespace py requester

/* Simple exception type */
exception Exception
{
    1: string msg
}

struct Request {
    1: string method,
    2: string url,
    3: map<string,string> data,
    4: map<string,string> cookies,
    5: bool no_cache
}

struct Response {
    1: i32 status_code,
    2: string url,
    3: map<string,string> headers,
    4: string content,
    5: bool from_cache,
    6: i32 timestamp
}


service Requester {
    /* does http request for resorce */
    Response urlopen(1: Request request)
    throws (1: Exception ex)
}
