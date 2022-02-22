Examples
========

The following snippets exemplify how to perform several extractions with querier.



Sample entries
--------------

This example shows how to take a sample from the twitter database and print it in a
human readable way::

    import querier as qr
    from pprint import pprint

    database_name = "twitter_2020"

    with qr.Connection(database_name, credentials_file) as con:
        result = con.extract_one(qr.Filter())
        pprint(result)

Due to the nature of MongoDB databases, entries can have missing fields (use
:py:meth:`querier.Filter.exists` to filter entries where a field is not missing).

It can be useful to sample a database this way first to know the entries' fields.



Extraction from a database
--------------------------

This example shows a way to extract tweets and store them in a *.gz* file. The tweets
extracted were twitted from spain in 2020 between March and September (note that we only
store the fields 'place', 'user' and 'id')::

    import querier as qr
    import json
    import gzip
    from datetime import datetime

    database_name = "twitter_2020"

    # Filter
    date_end = datetime(2020, 9, 30, 0, 0, 0) # September 2020
    date_start = datetime(2020, 3, 1, 0, 0, 0) # March 2020

    f = qr.Filter()
    f.exists('place')
    f.equals('place.country_code', 'ES')
    f.less_or_equals('created_at', date_end)
    f.greater_or_equals('created_at', date_start)

    # The results will have those fields only
    fields = ['place', 'user', 'id']

    # Extraction
    with gzip.open('twitter_2020_ES.gz', 'wb') as outfile:
        with qr.Connection(database_name) as con:

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

This example prints (in a human readable way) the tweet from Spain which was most marked
as favorite::

    import querier as qr
    from pprint import pprint

    year_start = 2018
    year_end = 2020
    fields = ['created_at', 'text', 'user', 'retweet_count', 'favorite_count']
    f = qr.Filter() # Empty filter
    max_tweet = None

    for year in range(year_start, year_end + 1):
        database_name = f"twitter_{year}"

        with qr.Connection(database_name) as con:
            result = con.extract(f, fields)
            for tweet in result:

                if max_tweet is None:
                    max_tweet = tweet
                    continue

                if tweet['favorite_count'] > max_tweet['favorite_count']:
                    max_tweet = tweet

    pprint(max_tweet)

It shows a way to extract entries from several databases using more than one connection.
Twitter databases are split by year and named ``twitter_{year}`` requiring more than one
Connection object to extract tweets from different years.



Group by a field and aggregate
------------------------------

Now imagine you would like to extract the places from tweets of a collection which pass
a given filter, and count how many tweets each place is attached to::

    import querier as qr

    tweets_filter = qr.Filter().equals("place.country_code", "FR")

    with qr.Connection("twitter_2020") as con:
        places = (
            con["western_europe"]
             .groupby("place.id", pre_filter=tweets_filter, allowDiskUse=True)
             .agg(
                 name=("place.name", "first"),
                 type=("place.place_type", "first"),
                 nr_tweets=("place.id", "count"),
                 bbox=("place.bounding_box.coordinates", "first"),
             )
        )

        # Get a list of dictionaries with the keys given in `.agg`, plus "_id"
        # corresponding to the grouped-by-key, here "place.id":
        places_dicts = list(places)

        # or iterate through `places` to modify/further filter each entry before keeping
        # them in memory.

The `.agg` method works on the model of named aggregations of
:py:meth:`pandas.core.groupby.DataFrameGroupBy.aggregate`, except we provide a
`NamedTuple` :py:meth:`querier.NamedAgg` with keywords `field` and `aggfunc`. For
reference see `pandas' user guide`_. In this here example we simply passed in tuples
whose first entry corresponds to the field and the second to the aggregation function,
which also works.

.. _pandas' user guide:
    https://pandas.pydata.org/pandas-docs/stable/user_guide/groupby.html?#named-aggregation



Geographic filters
--------------------------

If you want to select tweets with coordinates within a given place, let's say New York
City::

    import querier as qr
    import geopandas as gpd

    # Note the conversion to 'epsg:4326', the default (longitude, latitude) coordinate
    # reference system.
    nyc_boroughs = gpd.read_file(gpd.datasets.get_path('nybb')).to_crs('epsg:4326')
    f = qr.Filter()

here are several equivalent ways to generate the corresponding filter, first using the
full polygon::

    f.geo_within('coordinates', nyc_boroughs.unary_union)

which may prove rather costly given the complexity of the input polygon. To now generate
a rougher but simpler filter using NYC's bounding box::

    f.geo_within('coordinates', nyc_boroughs.total_bounds, geo_type='bbox')

or equivalently::

    from shapely.geometry import box

    f.geo_within('coordinates', box(*nyc_boroughs.total_bounds))
