namespace py requester

/* Simple exception type */
exception Exception
{
    1: string msg
}

struct Request {
    1: string url,
    2: optional string method,
    3: optional map<string,string> data,
    4: optional map<string,string> cookies,
    5: optional bool no_cache = 0
}

struct Response {
    1: string url,
    2: optional i32 status_code,
    3: optional map<string,string> headers,
    4: optional string content,
    5: optional bool from_cache,
    6: optional double response_time,
    7: optional double timestamp
}


service Requester {
    /* does http request for resorce */
    Response urlopen(1: Request request)
    throws (1: Exception ex)
}
