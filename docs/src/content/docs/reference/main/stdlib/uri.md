---
title: URI functions
---

XTDB has support for first-class URIs, with the following functions:

## Constructors

`URI 'https://…​'`
: URI constructor

`CAST('…​' AS URI)`, `'…​'::URI`
: cast a string to a URI

## Extraction functions

The following functions extract components from a URI:

`URI_SCHEME(URI 'https://xtdb.com')`
: returns the scheme of the URI (e.g. `https`)

`URI_USER_INFO(URI 'https://user:pass@xtdb.com')`
: returns the user info of the URI (e.g. `user:pass`)

`URI_HOST(URI 'https://xtdb.com')`
: returns the host of the URI (e.g. `xtdb.com`)

`URI_PORT(URI 'https://xtdb.com:8080')`
: returns the port of the URI (e.g. `8080`)

`URI_PATH(URI 'https://xtdb.com:8080/path/to/resource')`
: returns the path of the URI (e.g. `/path/to/resource`), or an empty
    string if the path is not specified in the URI.

`URI_QUERY(URI 'https://xtdb.com:8080/path/to/resource?query=string')`
: returns the query of the URI (e.g. `query=string`)

`URI_FRAGMENT(URI 'https://xtdb.com:8080/path/to/resource#fragment')`
: returns the fragment of the URI (e.g. `fragment`)

All functions (unless otherwise specified) return `NULL` if the component is not specified in the URI.
