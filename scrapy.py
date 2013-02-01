#!/usr/bin/python
# -*- coding: utf-8 -*-

# Author: Vladimir M. Zaytsev <zaytsev@usc.edu>
# URL: <http://cbg.isi.edu/>
# For license information, see LICENSE


import os
import sys
import logging
import datetime
import argparse
import traceback
import twstorage
import multiprocessing
import oauth2 as oauth
import anyjson as json

from collections import deque
from twisted.python import log
from twisted.internet import reactor
from twisted.web import server, resource
from twisted.internet.task import LoopingCall
from twisted.python.logfile import DailyLogFile
from twforce.streams import TwClientFactory, TwHandler, connect_api

global LOG_FILE

VERSION = "2.1.1"


def read_settings(filepath="scrapy-settings.json"):
    json_file = open(filepath, "r")
    settings = json.loads(json_file.read())
    json_file.close()
    return settings


def make_oauth_consumer(settings):
    token = settings["oauth"]["token"]
    secret = settings["oauth"]["secret"]
    return oauth.Consumer(token, secret)


def iso_time(ts):
    return ts.strftime("%Y-%m-%dT%H:%M:%S")


class TweetHandler(TwHandler):

    def __init__(self, scraper):
        TwHandler.__init__(self)
        self.scraper = scraper

    def connection_made(self):
        self.scraper.status = self.scraper.Status.CONNECTED
        self.scraper.ts_connect = datetime.datetime.utcnow()
        log.msg("Connection made %r" % self.scraper,
                logLevel=logging.WARNING)

    def connection_lost(self, reason):
        self.scraper.received = 0
        self.scraper.limits = 0
        self.scraper.status = self.scraper.Status.FAILED
        log.msg("Connection lost %r\nReason:%r" % (self.scraper, reason),
                logLevel=logging.WARNING)

    def handle(self, line):
        try:
            jsn = json.loads(line)
            if "text" in jsn:
                self.scraper.add_tweet(jsn)
            elif "limit" in jsn:
                self.scraper.add_limit(jsn)
        except Exception:
            pass

class ScraperState(object):

    class Status(object):

        CONNECTED = 1
        CONNECTING = 0
        FAILED = -1

    def __init__(self, name, token, filter, cache_location):
        self.handler = TweetHandler(self)
        self.name = name
        self.token = token
        self.filter = filter
        self.filter_id = filter["id"]
        self.status = self.Status.CONNECTING
        self.factory = None
        self.connector = None
        self.received = 0
        self.limits = 0
        self.last_received = None
        self.ts_start = datetime.datetime.utcnow()
        self.ts_connect = None
        self.errors = deque([], maxlen=32)
        self.cache = cache_location
        self.total_limits = 0
        self.total_received = 0
        self.rate = 0
        log.msg("Create new scraper %r" % self)

    def last_receiveds(self):
        if self.last_received:
            return iso_time(self.last_received)
        return None

    def ts_starts(self):
        return iso_time(self.ts_start)

    def connect(self, consumer):
        log.msg("Connect %r" % self, logLevel=logging.DEBUG)
        if not self.factory:
            self.factory = TwClientFactory.filter_streamer(
                consumer,
                self.token,
                self.handler,
                location=self.filter.get("location", []),
                track=self.filter.get("track", []),
                follow=self.filter.get("follow", []),
            )
        if self.connector:
            self.connector.connect()
        else:
            self.connector = connect_api(self.factory)

    def disconnect(self):
        log.msg("Disconnect %r" % self, logLevel=logging.DEBUG)
        if self.connector:
            self.connector.disconnect()
            self.factory.stopTrying()

    def add_tweet(self, tweet):
        self.received += 1
        self.total_received += 1
        self.cache.append((self.token.key, self.filter_id, tweet))
        self.last_received = datetime.datetime.utcnow()

    def add_limit(self, limit):
        limit_value = 0
        for n in limit["limit"].values():
            limit_value += n
        self.limits += limit_value
        self.total_limits += limit_value
        self.cache.append((self.token.key, self.filter_id, limit_value))
        self.last_received = datetime.datetime.utcnow()

    def get_rate(self):
        d = (datetime.datetime.utcnow() - self.ts_connect).seconds
        return float(self.received) / d * 60

    def __repr__(self):
        return u"<Scraper(name=%s, token=%s)>" % (self.name, self.token.key)


class ScrapyAPI(resource.Resource):
    isLeaf = True

    def __init__(self, consumer):
        resource.Resource.__init__(self)
        self.scrapers = {}
        self.consumer = consumer
        self.cache = deque([])
        init = lambda : twstorage.init(read_settings())
        self.storage_worker = multiprocessing.Pool(processes=1,
                                                   initializer=init)

    def __add_scrapers__(self, param_list):
        for param in param_list:
            token = oauth.Token(
                key=param["oauth"]["token"],
                secret=param["oauth"]["secret"]
            )
            location = tuple(param["filter"]["location"])
            name = param["name"]
            if token.key not in self.scrapers:
                filter = dict(location=location, id=param["filter"]["id"])
                new_scraper = ScraperState(name, token, filter, self.cache)
                self.scrapers[token.key] = new_scraper
                new_scraper.connect(self.consumer)

        return {"success": True}

    def __remove_scrapers__(self, params):
        for t in params:
            scraper = self.scrapers.get(t)
            if scraper:
                scraper.disconnect()
                del self.scrapers[t]
        return {"success": True}

    def __list_scrapers__(self):
        sc_list = []
        for s in self.scrapers.values():
            sc_list.append({
                "name": s.name,
                "token": s.token.key,
                "status": s.status,
                "ts_start": s.ts_starts(),
                "received": s.received,
                "total_received": s.total_received,
                "limits": s.limits,
                "total_limits": s.total_limits,
                "last_received": s.last_receiveds(),
                "rate": s.get_rate(),
                "filter": s.filter,
                "errors": list(s.errors),
            })
        return sc_list

    def render_GET(self, request):
        try:
            log.msg("Handle request: %s" % request.path, logLevel=logging.DEBUG)
            request.setHeader("Content-Type", "application/json")

            if request.path == "/add/":
                params = json.loads(request.args["data"][0])
                response = self.__add_scrapers__(params)
                return json.dumps(response)

            elif request.path == "/list/":
                response = self.__list_scrapers__()
                return json.dumps(response)

            elif request.path == "/remove/":
                params = json.loads(request.args["data"][0])
                response = self.__remove_scrapers__(params)
                return json.dumps(response)
            elif request.path == "/ping/":
                return "pong"
            elif request.path == "/log/":
                logfile = open("log/daily-log.log")
                log_message = logfile.read()
                logfile.close()
                return log_message
            else:
                log.msg("Wrong API path '%s'" % request.path,
                        logLevel=logging.DEBUG)
                return json.dumps({
                    "error": True,
                    "message": "Wrong API path '%s'" % request.path,
                })

        except Exception:
            log.msg("Error: %s" % traceback.format_exc(),
                    logLevel=logging.WARNING)
            return json.dumps({
                "error": True,
                "message": traceback.format_exc(),
            })


def collect_received(api):
    collected = []
    d_len = len(api.cache)
    while d_len:
        collected.append(api.cache.pop())
        d_len -= 1
    if collected:
        api.storage_worker.map_async(twstorage.save, [collected])


MSG = \
"""
\n\n
     _______.  ______ .______           ___      .______   ____    ____
    /       | /      ||   _  \         /   \     |   _  \  \   \  /   /
   |   (----`|  ,----'|  |_)  |       /  ^  \    |  |_)  |  \   \/   /
    \   \    |  |     |      /       /  /_\  \   |   ___/    \_    _/
.----)   |   |  `----.|  |\  \----. /  _____  \  |  |          |  |
|_______/     \______|| _| `._____|/__/     \__\ | _|          |__|
\n\n
Version: %s
Started on: %s UTC
PORT: %d
LOG: %r
PID: %d
DB: %s@%s:%s/%s\n
"""

if __name__ == "__main__":
    settings = read_settings()
    port = settings["api"].get("port", 8000) if "api" in settings else 8000
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", default=port, type=int,
                        help="HTTP API port")
    parser.add_argument("-l", "--log", type=int, default=1,
                        help="Log-file")
    args = parser.parse_args()
    api_port = args.port
    log_file = "daily-log.log" if args.log else sys.stderr
    log_file = log_file
    if log_file is not sys.stderr:
        log_file = DailyLogFile(log_file, "%s/log" % os.getcwd())
    MSG = MSG % (
        VERSION,
        datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        api_port,
        log_file,
        os.getpid(),
        settings["database"]["username"],
        settings["database"]["host"],
        settings["database"]["port"],
        settings["database"]["name"],
    )
    sys.stdout.write(MSG)
    consumer = make_oauth_consumer(settings)
    log.startLogging(log_file)
    api = ScrapyAPI(consumer)
    site = server.Site(api)
    reactor.listenTCP(api_port, site)
    lc = LoopingCall(lambda: collect_received(api))
    lc.start(settings["database"]["commit_delay"])
    reactor.run()

    log_file.close()