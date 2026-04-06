# Sklad Protocol

Currently Sklad uses a simple, line-based, JSON over TCP protocol for client-server communication.

The **default port** is `7733`. Each message (request and response) is a single JSON object terminated by a newline character.

## Requests

The request structure is as follows:

```
{
    "kind": <request kind id>
    "query": "<query string>"
    "timestamp": <timestamp>
}
```

There are 3 request kinds:

* 0 - query, a regular database query
* 1 - metric, a request to retrieve server metrics
* 2 - continue batch, a request to continue retrieving the results for an ongoing range query

## Responses

The server responds with a JSON object containing either the requested data or an error. The response structure is generic:

```
{
    "data": <response values if any>,
    "errors": <server error or null>
}
```

### Fields
* `data` - The result of the successful request. Its structure depends on the request kind. If an error occurs, this is typically `null`.

* `errors` - Present if the request failed. If the request was successful, this field is `null`.
