# CBG Scrapy


CBG Scrapy – is a simple HTTP server for asynchronous scrapping  data from Twitter API using Twisted library.

## Installation and running

```bash
$ python scrapy.py [-p <http API port>] [-l <path to log file>]
```

## HTTP API


* ###Adding scrapers

	Adds (activates) new scrapers.

	URI: `/add/`
	
	GET parameters:
	
	```json
	data:
	[
		{
			"name": "LA Scraper",
			"oauth": {
				"token": "<Twitter's OAuth token>",
				"secret": "<Twitter's OAuth secret>"
			},
			"filter": {
				"id": "Some integer, unique for each scraper",
				"location": [-122.75, 36.8, -121.75, 37.8],
			}
		}
	]
	```
	Response:
	
	```js
	{
		"error": true | false,
		"message": "Error message"
	}
	```

* ### Listing scrapers
	
	Returnes state of active scrapers.
	
	URI: `/list/`
	
	GET parameters:
	
	
	```
	none
	```
	
	Response:
	
	```js
	[
		{
			"name": "LA scraper",
			"token": "<Twitter's OAuth token>",
			"status": "connecting" | "connected" | "failed",
			"ts_start": "2012.12.12T12:12:00",
			"received": 10000,
			"total_received": 100000,
			"limits": 5000,
			"total_limits": 60000,
			"rate": 10.4,			
			"last_received": "2012.12.12T12:12:00",
			"filter": {
				"track": ["#Python", "#Haskell"],
				"follow": [1, 2, 4],
				"locations" [0, 0, 0, 0]
			},
			"errors": [
				{
					"message": "error message",
					"ts": "2012.12.12T12:12:00"
				}
			]
		}
	]
	```
* ### Removing scrapers
	
	Stops and removes active scrapers.
	
	URI: `/remove/`
	
	GET parameters:
	
	```js
	data:
	[
		"<Twitter's OAuth token>"
	]
	```
	
	Response:
	
	```js
	{
		"error": true | false,
		"message": "Error message"
	}
	```
* ### Ping

	Returns string `pong`.
	
	URI: `/ping/`
	
	GET parameters:
	
	```
	none
	```
	
	Response:
	
	```
	pong
	```

* ### Log
	
	Returns log string.
	
	URI: `/log/`
	
	GET parameters:
	
	```
	none
	```
