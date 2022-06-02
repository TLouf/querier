"""Module docstring."""
from __future__ import annotations

import configparser
import logging
import warnings
from functools import wraps
from os import getpid
from os.path import expanduser
from typing import TYPE_CHECKING, Any, NamedTuple

import pymongo

from querier.exceptions import (
    AuthentificationError,
    CredentialsError,
    InternalError,
    ServerError,
)
from querier.result import Result

if TYPE_CHECKING:
    from querier.filter import Filter

module_logger = logging.getLogger("querier")
AUTHENTIFICATION_FAILED = 18
UNAUTHORIZED_COMMAND = 13


class NamedAgg(NamedTuple):
    """NamedTuple describing an aggregation to pass on to :py:meth:`MongoGroupBy.agg`.

    Parameters:
        field (str): The name of the field on which to apply the aggregation.
        aggfunc (str):
            The name of the aggregation function to apply. Most common aggregation
            functions work ("sum", "min", "mean"...). For a full reference see
            https://docs.mongodb.com/manual/reference/operator/aggregation/group/#accumulator-operator
    """

    field: str
    aggfunc: str


def _pymongo_call(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        try:

            return f(*args, **kwargs)

        except pymongo.errors.OperationFailure as e:
            # A collection can deny querying to that user (system.profile)
            if e.code != UNAUTHORIZED_COMMAND:
                module_logger.error("Unhandled OperationFailure: {}".format(e))
                raise InternalError(e) from None

        except pymongo.errors.PyMongoError as e:
            module_logger.error("Unhandled PyMongoError: {}".format(e))
            raise InternalError(e) from None

        except Exception as ex:
            module_logger.error("Unknown exception: {}", ex)
            raise ex from None

    return decorated


class Connection:
    """Establishes a connection with a database and allows data retrieval.

    The first parameter (`dbnamecfg`) must be a valid database name. The
    database administrator should provide both the credentials and the database
    names you are allowed to access.

    Parameters:
        dbnamecfg (str): The name of the database
        credentials_path (str, default "~/.credentials.cfg"):
            Path to a configuration file (.cfg) with credentials for the database.

    Examples:
        The following snippet shows the most simple way to create a Connection::

            import querier as qr

            with qr.Connection(dbname) as con:
                # Use con

        where dbname is the name of a database.

    """

    def __init__(self, dbnamecfg: str, credentials_path: str = "~/.credentials.cfg"):
        con, db = self._create_connection(dbnamecfg, credentials_path)
        self._con = con
        self._db = db
        self._dbname = dbnamecfg
        self._result_pool: list[Result] = []
        self._test_connection()

    # Called on enter to 'with' keyword
    def __enter__(self):
        return self

    # Called on exit from 'with' keyword
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        self.close()

    def __getitem__(self, coll_names: str | list[str] | slice | None):
        return CollectionsAccessor(self, coll_names)

    def close(self):
        """Close the connection."""
        try:
            for result in self._result_pool:
                result.close()

            module_logger.debug("Closing connection")
            self._con.close()
        except AttributeError:
            # If initialization fails self._con does not exists
            pass

    @_pymongo_call
    def list_available_collections(self) -> list:
        """Return a list of available collections.

        MongoDB databases can be split by collections. For example, all tweets from
        USA are in collection 'northern_america' in twitter databases. Contact the
        database administrator to know how/if collections are semantically split.

        Extraction methods can be sped up by using a subset of collections.

        Returns:
            A list of all the available collection names.
        """
        names = self._db.list_collection_names()
        for n in names:
            if "system" in n:
                names.remove(n)
        return names

    def count_entries(
        self,
        filter: Filter | None = None,
        collections_subset: list | None = None,
    ) -> int:
        """Count the entries that matches a filter.

        Parameters:
            filter: Filter object to count entries or `None` (all entries).
            collections_subset: List of collections to extract from. A subset of
                :py:meth:`Connection.list_available_collections()` or `None`
                (all collections).

        Returns:
            The number of entries in `collection` matching `filter`.

        Examples:
            Count how many tweets from Spain in 2014 have more than 500
            favorites:

            >>> import querier as qr
            >>> f = qr.Filter()
            >>> f.greater_than('favorite_count', 500)
            >>> with qr.Connection('twitter_2014') as con:
            >>>     count = con.count_entries(f, collection='spain')
            3
        """
        return self[collections_subset].count_entries(filter=filter)

    def extract_one(
        self,
        filter: Filter | None = None,
        collections_subset: list | None = None,
    ) -> dict | None:
        """Extract an entry from the database that matches a filter.

        Parameters:
            filter: Filter used to test the entries or `None` (all entries)
            collections_subset: List of collections to extract from. A subset of
                :py:meth:`Connection.list_available_collections()` or `None`
                (all collections).

        Returns:
            Entry from the database or `None` if no entry matches the filter.
        """
        return self[collections_subset].extract_one(filter=filter)

    def extract(
        self,
        filter: Filter | None = None,
        fields: list | None = None,
        collections_subset: list | None = None,
    ) -> Result:
        """Extract entries from the database that matches a filter.

        To limit the number of entries that will be returned, use
        :py:meth:`Result.limit()`. As databases can contain a huge number of
        entries, it is advised to test the code with a limited result first.

        To iterate through the entries, see :py:class:`querier.Result`

        Parameters:
            filter: Filter to test the entries.
            fields: List of selected fields from the original database that the
                result dictionaries will have. This is useful when only a subset
                of the fields is needed.
            collections_subset: List of collections to extract from. A subset of
                :py:meth:`Connection.list_available_collections()` or `None`
                (all collections).

        Returns:
            Result enabling to iterate through the matching entries in the
            database, or `None` if no entry matches the filter.
        """
        return self[collections_subset].extract(filter=filter, fields=fields)

    def aggregate(
        self,
        pipeline: list[dict],
        collections_subset: list | None = None,
        **aggregate_kwargs,
    ) -> Result:
        """Extract entries from the database resulting from a processing pipeline.

        To limit the number of entries that will be returned, use
        :py:meth:`Result.limit()`. As databases can contain a huge number of entries,
        it is advised to test the code with a limited result first.

        To iterate through the entries, see :py:class:`querier.Result`.

        Parameters:
            pipeline: List of `aggregation pipeline stages`_.
            collections_subset:
                List of collections to extract from. A subset of
                :py:meth:`Connection.list_available_collections()` or `None` (all
                collections).
            **aggregate_kwargs:
                additional keyword arguments to pass on to
                :py:meth:`pymongo.collection.Collection.aggregate`

        Returns:
            Result enabling to iterate through the matching entries in the
            database, or `None` if no entry matches the filter.

        .. _aggregation pipeline stages:
            https://docs.mongodb.com/manual/reference/operator/aggregation-pipeline/
        """
        return self[collections_subset].aggregate(pipeline, **aggregate_kwargs)

    def groupby(
        self,
        field_name: str | list(str),
        collection: str,
        pre_filter: Filter | None = None,
        post_filter: Filter | None = None,
        **aggregate_kwargs,
    ) -> MongoGroupBy:
        """Group by a given field.

        Initialize an aggregation pipeline in the collections given by
        `collections_subset` (the default `None` meaning all available
        collections), in which we filter according to a `pre_filter`, group by
        the field `field_name`, and then filter according to a `post_filter`.
        The aggregations done in the groupby stage are specified by a subsequent
        call to :py:meth:`MongoGroupBy.agg`.

        Parameters:
            field_name: Name or list of names of the field(s) by which to group.
            collection: Name of the collection to perform the aggregation from.
            pre_filter: Filter to apply before the aggregation.
            post_filter: Filter to apply after the aggregation.
            silence_warning:
                If True, silence the warning about doing an aggregation on multiple
                collections.
            **aggregate_kwargs:
                additional keyword arguments to pass on to
                :py:meth:`pymongo.collection.Collection.aggregate`.

        Returns:
            A MongoGroupBy instance enabling aggregation by `field_name`.
        """
        return self[collection].groupby(
            field_name,
            pre_filter=pre_filter,
            post_filter=post_filter,
            **aggregate_kwargs,
        )

    def distinct(
        self,
        field_name: str,
        filter: Filter | None = None,
        collections_subset: list | None = None,
    ) -> set:
        """Return a set with all the possible values that the field can take.

        Parameters:
            field_name: The name of the field to test.
            filter: Filter to test the entries.
            collections_subset: List of collections to extract from. A subset of
                :py:meth:`Connection.list_available_collections()` or `None`
                (all collections).

        Returns:
            Set of distinct values.

        Examples:
            >>> import querier as qr
            >>> with qr.Connection('twitter_2020') as con:
            >>>     con.distinct('place.country')
            {'Spain', 'France', 'Portugal', 'Germany', ...}
        """
        return self[collections_subset].distinct(field_name, filter=filter)

    def _test_connection(self) -> bool:
        base_message = "Error accessing the database '" + self._dbname + "'."

        try:
            self._con.admin.command("ismaster")
            return True

        except pymongo.errors.OperationFailure as e:
            if e.code == AUTHENTIFICATION_FAILED:
                message = (
                    base_message
                    + " Credentials are wrong or the user "
                    + "does not has sufficient permissions."
                )
                raise AuthentificationError(message) from None

        except pymongo.errors.ServerSelectionTimeoutError:
            message = (
                base_message
                + " No Mongo server is available "
                + " at the host or port found in the credentials file"
            )
            raise ServerError(message) from None

        except pymongo.errors.ConnectionFailure:
            message = base_message + " The connection to the database failed"
            raise ServerError(message) from None

        return False

    def _create_connection(self, dbname, credentials_path):
        host, port, user, pwd = _parse_credentials_file(credentials_path, dbname)

        # Connect to DB
        mongoserver_uri = "mongodb://{u}:{w}@{h}:{p}/{db}?authSource={db}".format(
            db=dbname, u=user, p=port, w=pwd, h=host
        )
        connection = pymongo.MongoClient(
            host=mongoserver_uri, replicaSet=None, connect=True
        )

        module_logger.info("Started connection to... ")
        module_logger.info("    * Database name: " + dbname)
        module_logger.info("    * Host: " + host)
        module_logger.info("    * Port: " + port)
        module_logger.info("    * PID: " + repr(getpid()))

        return connection, connection[dbname]


class CollectionsAccessor:
    """Provides access to one or several collections of a database for data retrieval.

    An instance of this class is normally obtained through a :py:class:`Connection`
    instance, selecting with square brackets the collection(s), as one would select
    columns from a pandas DataFrame for instance.

    Parameters:
        connection (Connection): Database connection used to retrieve data.
        collections_subset (optional):
            Collections of the database from which to retrieve the data.

    Examples:
        To extract a single document from the collections `colls` of the database
        `dbname`::

            import querier as qr

            colls = ["collection A", "collection B"]
            with qr.Connection(dbname) as con:
                result = con[colls].extract_one()
    """

    def __init__(
        self,
        connection: Connection,
        collections_subset: str | list[str] | slice | None = None,
    ):
        if collections_subset is None:
            collections_subset = slice(None)
        if isinstance(collections_subset, slice):
            coll_names = connection._db.list_collection_names()[collections_subset]
        elif isinstance(collections_subset, str):
            coll_names = [collections_subset]
        else:
            coll_names = collections_subset

        self.collections = [connection._db[coll] for coll in coll_names]
        self.connection = connection

    def count_entries(self, filter: Filter | None = None) -> int:
        """Count the entries that matches a filter.

        Parameters:
            filter: Filter object to count entries or `None` (all entries).

        Returns:
            The number of entries in `collection` matching `filter`.

        Examples:
            Count how many tweets from Spain in 2014 have more than 500
            favorites:

            >>> import querier as qr
            >>> f = qr.Filter()
            >>> f.greater_than('favorite_count', 500)
            >>> with qr.Connection('twitter_2014') as con:
            >>>     count = con['spain'].count_entries(f)
            3
        """
        query = {} if filter is None else filter.get_query()

        @_pymongo_call
        def internal_count(colls, query):
            total = 0
            for coll in colls:
                if not query:  # empty query = {}
                    total += coll.estimated_document_count()
                else:
                    total += coll.count_documents(query)
            return total

        return internal_count(self.collections, query)

    def extract_one(
        self,
        filter: Filter | None = None,
    ) -> dict | None:
        """Extract an entry from the database that matches a filter.

        Parameters:
            filter: Filter used to test the entries or `None` (all entries)

        Returns:
            Entry from the database or `None` if no entry matches the filter.
        """
        result = None

        module_logger.debug("######### Begin extract one #########")
        query = {} if filter is None else filter.get_query()

        module_logger.debug(
            "dbname '{}' | process pid {}".format(self.connection._dbname, getpid())
        )
        module_logger.debug(query)

        @_pymongo_call
        def internal_extract_one(coll, query):
            return coll.find_one(query)

        for coll in self.collections:
            result = internal_extract_one(coll, query)
            if result is not None:
                module_logger.info("  => found in collection '{}'".format(coll.name))
                break

        return result

    def extract(
        self,
        filter: Filter | None = None,
        fields: list | None = None,
    ) -> Result:
        """Extract entries from the database that matches a filter.

        To limit the number of entries that will be returned, use
        :py:meth:`Result.limit()`. As databases can contain a huge number of
        entries, it is advised to test the code with a limited result first.

        To iterate through the entries, see :py:class:`querier.Result`

        Parameters:
            filter: Filter to test the entries.
            fields: List of selected fields from the original database that the
                result dictionaries will have. This is useful when only a subset
                of the fields is needed.

        Returns:
            Result enabling to iterate through the matching entries in the
            database, or `None` if no entry matches the filter.
        """
        cursors = []
        coll_names = []
        projection = _field_list_to_projection_dict(fields)

        module_logger.debug("######### Begin extraction #########")
        query = {} if filter is None else filter.get_query()

        module_logger.debug(
            "dbname '{}' | process pid {}".format(self.connection._dbname, getpid())
        )
        module_logger.debug(query)

        @_pymongo_call
        def internal_extract(coll, query, projection):
            return coll.find(filter=query, projection=projection)

        r = Result()
        for coll in self.collections:
            module_logger.debug("    -> Extract in {}".format(coll.name))

            result = internal_extract(coll, query, projection)

            if result is not None:
                cursors.append(result)
                coll_names.append(coll.name)

        # Adds all obtained cursors to the Result object
        r._add_cursors(cursors, coll_names)
        self.connection._result_pool.append(r)
        return r

    def aggregate(
        self,
        pipeline: list,
        **aggregate_kwargs,
    ) -> Result:
        """Extract entries from the database resulting from a processing pipeline.

        To limit the number of entries that will be returned, use
        :py:meth:`Result.limit()`. As databases can contain a huge number of entries,
        it is advised to test the code with a limited result first.

        To iterate through the entries, see :py:class:`querier.Result`

        Parameters:
            pipeline: List of `aggregation pipeline stages`_.
            **aggregate_kwargs:
                additional keyword arguments to pass on to
                :py:meth:`pymongo.collection.Collection.aggregate`.

        Returns:
            Result enabling to iterate through the matching entries in the
            database, or `None` if no entry matches the filter.
        """
        cursors = []
        coll_names = []

        module_logger.debug("######### Begin extraction #########")
        module_logger.debug(
            "dbname '{}' | process pid {}".format(self.connection._dbname, getpid())
        )

        @_pymongo_call
        def internal_aggregate(coll, pipeline, **kwargs):
            return coll.aggregate(pipeline, **kwargs)

        r = Result()
        for coll in self.collections:
            module_logger.debug("    -> Extract in {}".format(coll.name))

            result = internal_aggregate(coll, pipeline, **aggregate_kwargs)

            if result is not None:
                cursors.append(result)
                coll_names.append(coll.name)

        # Adds all obtained cursors to the Result object
        r._add_cursors(cursors, coll_names)
        self.connection._result_pool.append(r)
        return r

    def groupby(
        self,
        field_name: str | list(str),
        pre_filter: Filter | None = None,
        post_filter: Filter | None = None,
        silence_warning: bool = False,
        **aggregate_kwargs,
    ) -> MongoGroupBy:
        """Group by a given field.

        Initialize an aggregation pipeline in the collections given by
        `collections_subset` (the default `None` meaning all available
        collections), in which we filter according to a `pre_filter`, group by
        the field(s) `field_name`, and then filter according to a `post_filter`.
        The aggregations done in the groupby stage are specified by a subsequent
        call to :py:meth:`MongoGroupBy.agg`.

        Parameters:
            field_name: Name or list of names of the field(s) by which to group.
            pre_filter: Filter to apply before the aggregation.
            post_filter: Filter to apply after the aggregation.
            silence_warning:
                If True, silence the warning about doing an aggregation on multiple
                collections.
            **aggregate_kwargs:
                additional keyword arguments to pass on to
                :py:meth:`pymongo.collection.Collection.aggregate`.

        Returns:
            A MongoGroupBy instance enabling aggregation by `field_name`.
        """
        if aggregate_kwargs is None:
            aggregate_kwargs = {}

        if isinstance(field_name, str):
            field_name = [field_name]

        pipeline: list[dict[str, Filter | dict]] = []
        if pre_filter is not None:
            pipeline.append({"$match": pre_filter})

        group_stage = {"_id": {fn: "$" + fn.removeprefix("$") for fn in field_name}}
        pipeline.append({"$group": group_stage})

        if post_filter is not None:
            pipeline.append({"$match": post_filter})

        return MongoGroupBy(
            self, pipeline, silence_warning=silence_warning, **aggregate_kwargs
        )

    def distinct(self, field_name: str, filter: Filter | None = None) -> set:
        """Return a set with all the possible values that the field can take.

        Parameters:
            field_name: The name of the field to test.
            filter: Filter to test the entries.

        Returns:
            Set of distinct values.

        Examples:
            >>> import querier as qr
            >>> with qr.Connection('twitter_2020') as con:
            >>>     con.distinct('place.country')
            {'Spain', 'France', 'Portugal', 'Germany', ...}
        """
        query = {} if filter is None else filter.get_query()
        module_logger.debug(
            "######### Begin distinct('{}') #########".format(field_name)
        )
        module_logger.debug(
            "dbname '{}' | process pid {}".format(self.connection._dbname, getpid())
        )
        module_logger.debug(query)

        @_pymongo_call
        def internal_distinct(coll, field_name, query):
            return coll.distinct(field_name, filter=query)

        result: set[Any] = set()
        for coll in self.collections:
            d = internal_distinct(coll, field_name, query)
            if d is not None:
                module_logger.debug("Executed distinct in " + coll.name)
                result = result.union(set(d))

        return result


class MongoGroupBy:
    """Enables aggregation by a pre-determined field."""

    def __init__(
        self,
        collections_accessor: CollectionsAccessor,
        pipeline: list,
        silence_warning: bool = False,
        **agg_kwargs,
    ):
        self.collections_accessor = collections_accessor
        if len(self.collections_accessor.collections) > 1 and not silence_warning:
            warnings.warn(
                """
                    "Groupby operates on a per-collection basis, hence you may not
                    obtain the aggregated result you expect by passing more than one
                    collection. If you're aware of this and want to silence this
                    warning, pass `silence_warning=True`.
                """,
                RuntimeWarning,
            )
        self.pipeline = pipeline
        self.agg_kwargs = agg_kwargs

    def agg(self, **aggregations) -> Result:
        """
        Perform an aggregation over the grouped-by-field.

        Parameters:
            **aggregations:
                Works on the model of named aggregations of
                :py:meth:`pandas.core.groupby.DataFrameGroupBy.aggregate`, except we
                provide a :py:meth:`querier.NamedAgg` with keywords `field` and
                `aggfunc`. For reference see `pandas' user guide`_.

        Returns:
            Result over which to iterate to obtain the output of the aggregation.

        Examples:
            Count the number of tweets by place in "collection" of database
            "twitter_2020"::

                import querier as qr

                with qr.Connection("twitter_2020") as con:
                    con["collection"].groupby("place.id", allowDiskUse=True).agg(
                        name=qr.NamedAgg(field="place.name", aggfunc="first"),
                        nr_tweets=qr.NamedAgg(field="id", aggfunc="count"),
                    )

        .. _pandas' user guide:
            https://pandas.pydata.org/pandas-docs/stable/user_guide/groupby.html?highlight=filter#named-aggregation
        """
        group_stage = [stage for stage in self.pipeline if "$group" in stage][0]

        for output_field, agg_descr in aggregations.items():
            if not hasattr(agg_descr, "aggfunc"):
                agg_descr = NamedAgg(*agg_descr)
            aggfunc = agg_descr.aggfunc
            if not aggfunc.startswith("$"):
                aggfunc = "$" + aggfunc

            if aggfunc == "$count":
                aggfunc = "$sum"
                input_field = 1
            else:
                input_field = agg_descr.field
                if not input_field.startswith("$"):
                    input_field = "$" + input_field

            group_stage["$group"][output_field] = {aggfunc: input_field}

        return self.collections_accessor.aggregate(self.pipeline, **self.agg_kwargs)

    aggregate = agg


def _get_config_option(config, main_section, option) -> str:
    if not config.has_option(main_section, option):
        raise CredentialsError(
            "Option '{}' in credentials file is missing".format(option)
        ) from None

    return config.get(main_section, option)


def _find_section_from_suffixes(dbname, config):
    for section in config.sections():
        if config.has_option(section, "suffixes"):
            suffixes = config.get(section, "suffixes").split(",")

            for suffix in suffixes:
                if section + suffix == dbname:
                    return section
    return None


def _parse_credentials_file(credentials_path, dbname):
    expanded_path = expanduser(credentials_path)
    config = configparser.RawConfigParser(allow_no_value=True)

    try:

        with open(expanded_path) as ifh:
            config.read_file(ifh)

    except FileNotFoundError:
        raise CredentialsError("File '" + expanded_path + "' not found.") from None
    except OSError:
        raise CredentialsError("Error reading file '" + expanded_path + "'.") from None
    except configparser.Error as err:
        module_logger.error("Error parsing file: " + err)
        raise CredentialsError("Error parsing file '" + expanded_path + "'.") from None
    except Exception as err:
        module_logger.error(
            "Unhandled exception reading '" + expanded_path + "': " + err
        )

    if config.has_section(dbname):
        section_name = dbname
    else:
        section_name = _find_section_from_suffixes(dbname, config)

    if section_name is None:
        raise CredentialsError(
            "Database name '{}' not found in the credentials file '{}'".format(
                dbname, expanded_path
            )
        )

    dbtype = _get_config_option(config, section_name, "type")
    user = _get_config_option(config, section_name, "ruser")
    pwd = _get_config_option(config, section_name, "rpwd")

    if not config.has_section(dbtype):
        raise CredentialsError(
            "Section '{}' is missing in file '{}' (required for DB '{}')".format(
                dbtype, expanded_path, dbname
            )
        )

    host = _get_config_option(config, dbtype, "host")
    port = _get_config_option(config, dbtype, "port")

    return host, port, user, pwd


def _field_list_to_projection_dict(fields: list | None):
    """Convert a list of field names to a pymongo-formatted projection dict."""
    if fields is None or len(fields) == 0:
        return None

    proj = {f: 1 for f in fields}
    return proj
