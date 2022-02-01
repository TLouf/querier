Examples
========

The following snippets exemplify how to perform several extractions with querier.


Sample entries
--------------

This example shows how to take a sample from the twitter database and print it in a
human readable way::

    import querier
    from pprint import pprint

    database_name = "twitter_2020"

    with querier.Connection(database_name, credentials_file) as con:
        result = con.extract_one(querier.Filter())
        pprint(result)

Due to the nature of MongoDB databases, entries can have missing fields (use
:py:meth:`querier.Filter.exists` to filter entries where a field is not missing).

It can be useful to sample a database this way first to know the entries' fields.


Extraction from a database
--------------------------

This example shows a way to extract tweets and store them in a *.gz* file. The tweets
extracted were twitted from spain in 2020 between March and September (note that we only
store the fields 'place', 'user' and 'id')::

    import querier
    import json
    import gzip
    from datetime import datetime

    database_name = "twitter_2020"

    # Filter
    date_end = datetime(2020, 9, 30, 0, 0, 0) # September 2020
    date_start = datetime(2020, 3, 1, 0, 0, 0) # March 2020

    f = querier.Filter()
    f.exists('place')
    f.equals('place.country_code', 'ES')
    f.less_or_equals('created_at', date_end)
    f.greater_or_equals('created_at', date_start)

    # The results will have those fields only
    fields = ['place', 'user', 'id']

    # Extraction
    with gzip.open('twitter_2020_ES.gz', 'wb') as outfile:
        with querier.Connection(database_name) as con:

            tweets_es = con.extract(f, fields).limit(100)

            for tweet in tweets_es:
                tweet_str = json.dumps(tweet, default=str)
                outfile.write((tweet_str + '\n').encode('utf-8'))


As databases can be (and often are) massive, it is advised to limit the selected fields
and store entries in files whenever its strictly required. To limit the extraction is a
good practice when testing code that extracts data from the database (as databases can
contain up to millions of entries).


Multiple Connections
--------------------


This example prints (in a human readable way) the tweet from Spain with the most
favorite number::

    import querier
    from pprint import pprint

    year_start = 2018
    year_end = 2020
    fields = ['created_at', 'text', 'user', 'retweet_count', 'favorite_count']
    f = querier.Filter() # Empty filter
    max_tweet = None

    for year in range(year_start, year_end + 1):
        database_name = "twitter_" + repr(year)

        with querier.Connection(database_name) as con:
            result = con.extract(f, fields)
            for tweet in result:

                if max_tweet is None:
                    max_tweet = tweet
                    continue

                if tweet['favorite_count'] > max_tweet['favorite_count']:
                    max_tweet = tweet


    pprint(max_tweet)

It shows a way to extract entries from several databases using more than one connection.
Twitter databases are split by year and named twitter_{year} requiring more than one
Connection object to extract tweets from different years.
