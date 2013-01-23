BEGIN

-- Table for actual tweets.
CREATE TABLE "tweets" (
    "id" bigint NOT NULL PRIMARY KEY,
    "user_id" bigint NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    "geo"
    "text" varchar(140) NOT NULL
);

-- Here we will store limit messages and where they 
-- came from.
CREATE TABLE "limits" (
    "id" serial NOT NULL PRIMARY KEY,
    "filter_id" integer, -- Here we store from which fiter limit message came from.
    "timestamp" timestamp with time zone NOT NULL,
    "value" integer NOT NULL
);

-- Raw will be here.
CREATE TABLE "tweet_json" (
    "id" bigint NOT NULL PRIMARY KEY,
    "filter_id" integer,
    "json" text NOT NULL
);

-- PostGIS function to add a field which will store geo data of tweets.
SELECT AddGeometryColumn('public', 'tweets', 'geo', 4326, 'POINT', 2);

COMMIT;