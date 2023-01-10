Quickstart
==================

Installation
------------

To install the package from the source repository, execute the following command:

.. code:: bash

    pip install git+https://github.com/TLouf/querier.git#egg=querier


You can then import the module to check it was properly installed::

    import querier as qr



Credentials file
----------------

A credentials file is required to access any database (next to the database name). The database administrator should provide
your username and password (among other required parameters).

A credentials file is a config file (`CFG file format <https://en.wikipedia.org/wiki/Configuration_file>`_) used by Querier to
access the databases. It contains two types of sections: sources, which defines where the data should be retrieved from and
specific databases.

An example of a credentials file is shown below:

.. code-block:: cfg

    [mongodb]
    host=mongo0.ifisc.lan,mongo1.ifisc.lan
    port=27017

    [twitter]
    suffixes=_2014,_2015,_2016
    type=mongodb
    ruser=<twitter_username>
    rpwd=<twitter_password>

In this example, the source is MongoDB server (section [mongodb]) and the specific database is [twitter]. Notice that
the [twitter] section is a [mongodb] database (defined by the field 'type').

For example, we could append another specific database for the same source:

.. code-block:: cfg

    [flightradar]
    type=mongodb
    ruser=<flightradar_username>
    rpwd=<flightradar_password>


.. note::
    The default location of the credentials file is your home directory (~/.credentials.cfg).
    If the file's name starts with a point ('.') character it becomes hidden to file
    explorers (option '-a' of ls lists hidden files in a directory)


.. note::
    Databases sections can have an optional field called 'suffixes' (see [twitter] in the example above).
    They define several databases that are similar. In the previous example, the section [twitter] grants
    the user access to databases:

    * twitter_2014
    * twitter_2015
    * twitter_2016

    When using any extraction method from :py:class:`querier.Connection`


* If the file is missing or its format is incorrect, a :py:class:`querier.CredentialsError` will be raised.
* If the credentials in the file are incorrect or not authorized to read a database, a :py:class:`querier.AuthentificationError` will be raised instead.

For more details, see :py:class:`querier.Connection`.

Connect to a MongoDB database
-----------------------------
A :py:class:`querier.Connection` object is required to retrieve data from
a database. To create it, a credentials file and a database name are required.
The list of databases you are allowed to access will be provided by the database administrator.

To start a new connection there are two ways:

    * :py:class:`querier.Connection` supports the python's 'with' keyword. It
      should be prioritized as it will close the connection automatically::

        import querier as qr
        with qr.Connection('twitter_2020') as con:
            # Use con

    * It can be instantiated and then closed manually using :py:meth:`querier.Connection.close()`::

        import querier as qr
        con = qr.Connection('twitter_2020')
        # Use con
        con.close()

Both examples create an object called **con** of type :py:class:`querier.Connection`, use it to extract data
and then close it.

The constructor starts a process to connect to the database .
This process can be resolved instantaneously or, at most, in 30 seconds.
If the connection process was successful the Connection object can be used to extract data from the database.
Otherwise an appropriate exception will be raised. (see :doc:`errors`)


Extract from a collection
-------------------------
Each database may contain several collections. To extract data from a specific
collection, you can select it with square brackets::

    with qr.Connection('twitter_2020') as con:
        result = con['collection_name'].extract(...)

which calls the :py:meth:`querier.CollectionsAccessor.extract()` method, equivalent to
providing `collections_subset='collection_name'` to the
:py:meth:`querier.Connection.extract()` method.

To know what collections are available in the database, you can use the
:py:meth:`querier.Connection.list_available_collections()` method::

    with qr.Connection('twitter_2020') as con:
        print(con.list_available_collections())


Database format
---------------

The entries in a MongoDB database are stored in a similar format to python dictionaries.
Each entry is a collection of fields with an associated value (which can be a simple or
composed type or even another dictionary). Here's an example of an entry from the
twitter database::

    {
        'created_at': datetime.datetime(2020, 1, 4, 13, 49, 59),
        'favorite_count': 0,
        'favorited': False,
        'lang': 'es',
        'place': {'attributes': {},
           'bounding_box': {'coordinates': [[[-109.479171, -56.557358],
                                             [-109.479171, -17.497384],
                                             [-66.15203, -17.497384],
                                             [-66.15203, -56.557358]]],
                            'type': 'Polygon'},
           'country': 'Chile',
           'country_code': 'CL',
           'full_name': 'Chile',
           'id': '47a3cf27863714de',
           'name': 'Chile',
           'place_type': 'country',
           'url': 'https://api.twitter.com/1.1/geo/id/47a3cf27863714de.json'},

        . . .
    }


Entries are returned by querier as python dictionaries. You can access a field by
its name::

    >>> tweet['created_at']
    datetime.datetime(2020, 1, 4, 13, 49, 59)

    >>> tweet['place']['bounding_box']
    {
        'coordinates': [[[-109.479171, -56.557358],
                        [-109.479171, -17.497384],
                        [-66.15203, -17.497384],
                        [-66.15203, -56.557358]]],
        'type': 'Polygon'
    }


The different operations to extract entries from the database are documented and explained in
:py:class:`querier.Connection`


Creating a filter
-----------------

To retrieve data from a database a :py:class:`querier.Filter` is required. They are used
to retrieve entries with special conditions.

The most simple filter is the empty filter::

    import querier as qr
    f = qr.Filter()

It will make :py:meth:`querier.Connection.extract` method to return all entries in the database as no condition is defined in the filter.

Filter methods can be used (see :py:class:`querier.Filter`) to add simple
conditions that test a particular field from the database.

Example of a filter::

    import querier as qr
    f = qr.Filter()
    f.greater_than('retweet_count', 500)
    f.less_than('retweet_count', 1000)
    f.any_of('place.country_code', ['ES', 'FR'])

This filter will only allow tweets (entries) from Spain or France with a number
of retweets between 500 and 1000.


.. note::
    To identify nested fields, the dot notation ('.') can be used. In the previous
    example a condition is added to the field 'place.country_code'. It refers to the
    field *country_code* which is subfield from the field named *place*.


See :doc:`examples` to get several code snippets that use querier to extract data.
The full list of classes and methods are documented in :doc:`api`
