# Functions for working with URLs

All these functions don't follow the RFC. They are maximally simplified for improved performance.

## Functions that Extract Parts of a URL

If the relevant part isn't present in a URL, an empty string is returned.

### protocol

Extracts the protocol from a URL.

Examples of typical returned values: http, https, ftp, mailto, tel, magnet...

### domain

Extracts the hostname from a URL.

```
domain(url)
```

**Parameters**

- `url` — URL. Type: [String](../../data_types/string.md).


The URL can be specified with or without a scheme. Examples:

```
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://yandex.com/time/
```

For these examples, the `domain` function returns the following results:

```
some.svn-hosting.com
some.svn-hosting.com
yandex.com
```

**Returned values**

- Host name. If ClickHouse can parse the input string as a URL.
- Empty string. If ClickHouse can't parse the input string as a URL.

Type: `String`.

**Example**

```sql
SELECT domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')
```
```text
┌─domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')─┐
│ some.svn-hosting.com                                   │
└────────────────────────────────────────────────────────┘
```

### domainWithoutWWW

Returns the domain and removes no more than one 'www.' from the beginning of it, if present.

### topLevelDomain

Extracts the the top-level domain from a URL.

```
topLevelDomain(url)
```

**Parameters**

- `url` — URL. Type: [String](../../data_types/string.md).

The URL can be specified with or without a scheme. Examples:

```
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://yandex.com/time/
```

**Returned values**

- Domain name. If ClickHouse can parse the input string as a URL.
- Empty string. If ClickHouse cannot parse the input string as a URL.

Type: `String`.

**Example**

```sql
SELECT topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk')
```
```text
┌─topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk')─┐
│ com                                                                │
└────────────────────────────────────────────────────────────────────┘
```

### firstSignificantSubdomain

Returns the "first significant subdomain". This is a non-standard concept specific to Yandex.Metrica. The first significant subdomain is a second-level domain if it is 'com', 'net', 'org', or 'co'. Otherwise, it is a third-level domain. For example, firstSignificantSubdomain ('<https://news.yandex.ru/>') = 'yandex ', firstSignificantSubdomain ('<https://news.yandex.com.tr/>') = 'yandex '. The list of "insignificant" second-level domains and other implementation details may change in the future.

### cutToFirstSignificantSubdomain

Returns the part of the domain that includes top-level subdomains up to the "first significant subdomain" (see the explanation above).

For example, `cutToFirstSignificantSubdomain('https://news.yandex.com.tr/') = 'yandex.com.tr'`.

### path

Returns the path. Example: `/top/news.html` The path does not include the query string.

### pathFull

The same as above, but including query string and fragment. Example: /top/news.html?page=2\#comments

### queryString

Returns the query string. Example: page=1&lr=213. query-string does not include the initial question mark, as well as \# and everything after \#.

### fragment

Returns the fragment identifier. fragment does not include the initial hash symbol.

### queryStringAndFragment

Returns the query string and fragment identifier. Example: page=1\#29390.

### extractURLParameter(URL, name)

Returns the value of the 'name' parameter in the URL, if present. Otherwise, an empty string. If there are many parameters with this name, it returns the first occurrence. This function works under the assumption that the parameter name is encoded in the URL exactly the same way as in the passed argument.

### extractURLParameters(URL)

Returns an array of name=value strings corresponding to the URL parameters. The values are not decoded in any way.

### extractURLParameterNames(URL)

Returns an array of name strings corresponding to the names of URL parameters. The values are not decoded in any way.

### URLHierarchy(URL)

Returns an array containing the URL, truncated at the end by the symbols /,? in the path and query-string. Consecutive separator characters are counted as one. The cut is made in the position after all the consecutive separator characters.

### URLPathHierarchy(URL)

The same as above, but without the protocol and host in the result. The / element (root) is not included. Example: the function is used to implement tree reports the URL in Yandex. Metric.

```
URLPathHierarchy('https://example.com/browse/CONV-6788') =
[
    '/browse/',
    '/browse/CONV-6788'
]
```

### decodeURLComponent(URL)

Returns the decoded URL.
Example:

``` sql
SELECT decodeURLComponent('http://127.0.0.1:8123/?query=SELECT%201%3B') AS DecodedURL;
```

```
┌─DecodedURL─────────────────────────────┐
│ http://127.0.0.1:8123/?query=SELECT 1; │
└────────────────────────────────────────┘
```

## Functions that remove part of a URL.

If the URL doesn't have anything similar, the URL remains unchanged.

### cutWWW

Removes no more than one 'www.' from the beginning of the URL's domain, if present.

### cutQueryString

Removes query string. The question mark is also removed.

### cutFragment

Removes the fragment identifier. The number sign is also removed.

### cutQueryStringAndFragment

Removes the query string and fragment identifier. The question mark and number sign are also removed.

### cutURLParameter(URL, name)

Removes the 'name' URL parameter, if present. This function works under the assumption that the parameter name is encoded in the URL exactly the same way as in the passed argument.


[Original article](https://clickhouse.yandex/docs/en/query_language/functions/url_functions/) <!--hide-->
