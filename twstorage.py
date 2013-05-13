# -*- coding: utf-8 -*-

# Gambit collector
#
# Copyright (C) USC Information Sciences Institute
# Author: Vladimir M. Zaytsev <zaytsev@usc.edu>
# URL: <http://cbg.isi.edu/>
# For license information, see LICENSE


import datetime
import anyjson as json

global STORAGE
global SQL_MOVE


SQL_MOVE = \
"""
BEGIN WORK;
LOCK TABLE t2_tmp_tweet IN SHARE ROW EXCLUSIVE MODE;
UPDATE t2_tmp_tweet SET status=1 WHERE STATUS=0;
INSERT INTO t2_tweet (
    SELECT id, user_id, timestamp, text, geo 
    FROM t2_tmp_tweet
    WHERE status = 1
);
UPDATE t2_tmp_tweet SET status=2 WHERE STATUS=1;
DELETE FROM t2_tmp_tweet WHERE status=2 and timestamp < current_date - interval '7 days';
COMMIT WORK;
"""




class TwStorage(object):

    def __init__(self, settings):

        from dateutil import tz
        from sqlalchemy import DateTime
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from sqlalchemy.ext.declarative import declarative_base
        from sqlalchemy import BigInteger, MetaData, Column, Integer, String
        from sqlalchemy import Text
        from geoalchemy2 import Geometry

        passwd = settings["database"]["password"]
        user = settings["database"]["username"]
        name = settings["database"]["name"]
        host = settings["database"]["host"]
        port = settings["database"]["port"]

        db_url = "postgresql+psycopg2://%s:%s@%s:%s/%s" % \
                 (user, passwd, host, port, name)

        self.engine = create_engine(db_url, echo=True, pool_size=8, pool_recycle=1800)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        self.metadata = MetaData(self.engine)
        self.Base = declarative_base(metadata=self.metadata)

        class Limit(self.Base):
            __tablename__ = settings["database"]["limit_table"]
            id = Column(Integer, primary_key=True)
            filter_id = Column(Integer, nullable=False)
            value = Column(Integer, nullable=False, default=0)
            timestamp = Column(DateTime, default=datetime.datetime.now())

        class Tweet(self.Base):
            __tablename__ = settings["database"]["tweet_table"]
            id = Column(BigInteger, primary_key=True)
            user_id = Column(BigInteger, nullable=False)
            timestamp = Column(DateTime, default=datetime.datetime.now())
            text = Column(String(140), nullable=False)
            geo = Column(Geometry(geometry_type='POINT', srid=4326), nullable=True)

        class TweetJson(self.Base):
            __tablename__ = settings["database"]["jsons_table"]
            id = Column(BigInteger, primary_key=True)
            filter_id = Column(Integer, nullable=False)
            json = Column(Text, nullable=False)

        self.Limit = Limit
        self.Tweet = Tweet
        self.TweetJson = TweetJson
        self.Point = lambda x, y: "SRID=4326;POINT(%f %f)" % (x, y)
        self.tz = tz.gettz("UTC")
        self.max_text_len = self.Tweet.text.property.columns[0].type.length


def init(settings):
    global STORAGE
    STORAGE = TwStorage(settings)


def save(cache):
    try:
        global STORAGE
        global SQL_MOVE

        print "collected: %s " % len(cache)

        limits = []
        tweets = []
        tjsons = []

        for token, filter_id, obj in cache:

            if isinstance(obj, int):
                limit_value = obj
                limit = STORAGE.Limit(filter_id=filter_id, value=limit_value)
                limits.append(limit)

            elif isinstance(obj, dict):

                geo = None
                if "geo" in obj and obj["geo"] and \
                   "type" in obj["geo"] and obj["geo"]["type"] == "Point":
                    lat, lng = obj["geo"]["coordinates"]
                    geo = STORAGE.Point(lat, lng)

                ts = datetime.datetime.strptime(obj["created_at"],
                                                "%a %b %d %H:%M:%S +0000 %Y")
                text = obj["text"]
                if len(text) > STORAGE.max_text_len:
                    text = text[0:STORAGE.max_text_len]

                tweet = STORAGE.Tweet(
                    id=obj["id"],
                    user_id=obj["user"]["id"],
                    timestamp=ts.replace(tzinfo=STORAGE.tz),
                    text=text
                )

                if geo is not None:
                    tweet.geo = geo

                tjson = STORAGE.TweetJson(
                    id=obj["id"],
                    filter_id=filter_id,
                    json=json.dumps(obj)
                )

                tweets.append(tweet)
                tjsons.append(tjson)

        STORAGE.session.add_all(limits)
        STORAGE.session.add_all(tweets)
        STORAGE.session.add_all(tjsons)
        STORAGE.session.commit()
        
        STORAGE.engine.execute(SQL_MOVE)

    except Exception:
        import traceback
        print traceback.format_exc()
        STORAGE.session.rollback()