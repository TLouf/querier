"""Module docstring."""
from __future__ import annotations

import configparser
import logging
from os.path import expanduser
from os import getpid
from functools import wraps
from typing import Hashable, NamedTuple, TYPE_CHECKING

import pymongo

from .exceptions import (
    CredentialsError,
    AuthentificationError,
    ServerError,
    InternalError,
)
from .result import Result

if TYPE_CHECKING:
    from .filter import Filter

module_logger = logging.getLogger("querier")
AUTHENTIFICATION_FAILED = 18
UNAUTHORIZED_COMMAND = 13


class NamedAgg(NamedTuple):
    field: Hashable
    aggfunc: str


def _pymongo_call(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        try:

            return f(*args, **kwargs)

        except pymongo.errors.OperationFailure as e:
            # A collection can deny querying to that user (system.profile)
            if e.code != UNAUTHORIZED_COMMAND:
                module_logger.error("Unhandled OperationFailure: {}"\
                    .format(e))
                raise InternalError(e) from None
    
        except pymongo.errors.PyMongoError as e:
            module_logger.error("Unhandled PyMongoError: {}"\
                .format(e))
            raise InternalError(e) from None

        except Exception as ex:
            module_logger.error("Unknown exception: {}", ex)
            raise ex from None

    return decorated


class Connection:
    """Establishes a connection with a database and allows data retrieval.
    
    The constructor takes two parameters:

    :Parameters:
        - dbnamecfg: The name of the database
        - credentials_path [defaults='~/.credentials.cfg']: Path to a configuration file (.cfg) with credentials for the database.
            
    .. note::
        The first parameter (*dbnamecfg*) must be a valid database name. The database administrator
        should provide both the credentials and the database names you are allowed to access.


    When a Connection object is created the connection process will be initiated. This process 
    will try to connect to the database with a maximum deadline of 30 seconds.  

    A Connection should be closed when it's no longer in use, either by using 
    the method :py:meth:`Connection.close()` or the python's 'with' keyword 
    (being the later preferred whenever it's possible).

    The following snippet shows the most simple way to create a Connection::

        from querier import Connection

        with Connection(dbname) as con:
            # Use con
    
    where dbname is the name of a database.

    """

    def __init__(self, dbnamecfg: str, credentials_path: str = "~/.credentials.cfg"):
        """Create and initiate a connection to a database.

        :param dbnamecfg: name of the database
        :param credentials_path: path to a configuration file with the credentials
        to read the database
        """
        con, db = self._create_connection(dbnamecfg, credentials_path)
        self._con = con
        self._db = db
        self._dbname = dbnamecfg
        self._result_pool = []
        self._test_connection()


    # Called on enter to 'with' keyword
    def __enter__(self):
        return self


    # Called on exit from 'with' keyword
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


    def __del__(self):
        self.close()


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


    def count_entries(
        self,
        filter: Filter | None = None,
        collection: str | None = None
    ) -> int:
        """Count the entries that matches a filter.

        Example: Count how many tweets from Spain in 2014 have more than 500 favorites

        >>> from querier import *
        >>> f = Filter()
        >>> f.greater_than('favorite_count', 500)
        >>> with Connection('twitter_2014') as con:
        >>>     count = con.count_entries(f, collection='spain')
        3

        :param filter: Filter object to count entries or None (all entries)
        :param collection: A collection name from :py:meth:`Connection.list_available_collections()` or None (all collections)
        """
        query = {} if filter is None else filter.get_query()

        @_pymongo_call
        def internal_count(self, colls, query):
            total = 0
            for coll in colls:
                if not query: # empty query = {}
                    total += coll.estimated_document_count()
                else:
                    total += coll.count_documents(query)
            return total
        
        if collection is not None:
            coll_names = [collection]
        else:
            coll_names = self.list_available_collections()    

        colls = []
        for name in coll_names:
            colls.append(self._db[name])

        return internal_count(self, colls, query)


    def extract_one(
        self,
        filter: Filter | None = None,
        collections_subset: list | None = None,
    ) -> dict:
        """Extract an entry from the database that matches a filter.

        :param filter: filter that will be used to test the entries
        :param collections_subset: list of collections to extract from. A subset of :py:meth:`Connection.list_available_collections()`
        :return: entry from the database or None if no entry matches the filter
        """
        result = None
        
        module_logger.debug("######### Begin extract one #########")
        query = {} if filter is None else filter.get_query()
        
        module_logger.debug("dbname '{}' | process pid {}".format(self._dbname, getpid()))
        module_logger.debug(query)

        collections = collections_subset
        if collections is None:
            collections = self._db.list_collection_names()

        @_pymongo_call
        def internal_extract_one(self, coll, query):
            return self._db[coll].find_one(query)

        for coll in collections:
            result = internal_extract_one(self, coll, query)
            if result is not None:
                module_logger.info("\  => found in collection '{}'".format(coll))
                break

        return result

    @_pymongo_call
    def list_available_collections(self) -> list:
        """Return a list of available collections.

        MongoDB databases can be splitted by collections. For example, all tweets from USA are
        in collection 'northern_america' in twitter databases. Contact the database administrator 
        to know how/if collections are semantically splitted.

        Extraction methods can be sped up by using a subset of collections.

        :return: a list of all the available collection names
        """
        names = self._db.list_collection_names()
        for n in names:
            if 'system' in n:
                names.remove(n)
        return names


    def extract(
        self,
        filter: Filter | None = None,
        fields: list | None = None,
        collections_subset: list | None = None,
    ) -> Result : 
        """Extract entries from the database that matches a filter.

        To limit the number of entries that will be returned, use :py:meth:`Result.limit()`. As 
        databases can contain a huge number of entries, it is advised to test the code 
        with a limited result first.

        To iterate through the entries, see :py:class:`querier.Result`

        :param filter: filter to test the entries
        :param fields: List of selected fields from the original database that the result dictionaries 
                       will have. This is useful when only a subset of the fields is needed.
        :param collections_subset: list of collections to extract from. A subset of :py:meth:`Connection.list_available_collections()`
        :return: entry from the database or None if no entry matches the filter
        """
        cursors = []
        colls = self._db.list_collection_names()
        projection = Connection._field_list_to_projection_dict(fields)

        module_logger.debug("######### Begin extraction #########")
        query = {} if filter is None else filter.get_query()
        
        module_logger.debug("dbname '{}' | process pid {}".format(self._dbname, getpid()))
        module_logger.debug(query)

        collections = collections_subset
        if collections is None:
            collections = self._db.list_collection_names()

        @_pymongo_call
        def internal_extract(self, coll, query, projection):
            return self._db[coll].find(filter=query, projection=projection)

        r = Result()
        for coll in collections:
            module_logger.debug("    -> Extract in {}".format(coll))

            result = internal_extract(self, coll, query, projection)

            if result is not None:
                cursors.append(result)
                colls.append(coll)
            

        # Adds all obtained cursors to the Result object
        r._add_cursors(cursors, colls)
        self._result_pool.append(r)
        return r


    def aggregate(
        self,
        pipeline: list,
        collections_subset: list | None = None,
        **aggregate_kwargs,
    ) -> Result: 
        """Extract entries from the database resulting from a processing pipeline.

        To limit the number of entries that will be returned, use :py:meth:`Result.limit()`. As 
        databases can contain a huge number of entries, it is advised to test the code 
        with a limited result first.

        To iterate through the entries, see :py:class:`querier.Result`

        :param pipeline: filter to test the entries
        :param collections_subset: list of collections to extract from. A subset of :py:meth:`Connection.list_available_collections()`
        :param aggregate_kwargs: additional keyword arguments to pass on to :py:meth:`pymongo.collection.Collection.aggregate`
        :return: entry from the database or None if no entry matches the filter
        """
        cursors = []
        colls = self._db.list_collection_names()

        module_logger.debug("######### Begin extraction #########")
        
        module_logger.debug("dbname '{}' | process pid {}".format(self._dbname, getpid()))

        collections = collections_subset
        if collections is None:
            collections = self._db.list_collection_names()

        @_pymongo_call
        def internal_aggregate(self, coll, pipeline, **kwargs):
            return self._db[coll].aggregate(pipeline, **kwargs)

        r = Result()
        for coll in collections:
            module_logger.debug("    -> Extract in {}".format(coll))

            result = internal_aggregate(self, coll, pipeline, **aggregate_kwargs)

            if result is not None:
                cursors.append(result)
                colls.append(coll)
            

        # Adds all obtained cursors to the Result object
        r._add_cursors(cursors, colls)
        self._result_pool.append(r)
        return r

    def groupby(
        self,
        field_name: str,
        pre_filter: Filter | None = None,
        post_filter: Filter | None = None,
        collections_subset: list | None = None,
        **aggregate_kwargs,
    ) -> MongoGroupby:
        '''Group by a given field and return aggregate results.

        Initialize an aggregation pipeline in the collections given by
        `collections_subset` (the default `None` meaning all available
        collections), in which we filter according to a `pre_filter`, group by
        the field `field_name`, and then filter according to a `post_filter`.
        The aggregations done in the groupby stage are specified by a subsequent
        call to :py:meth:`MongoGroupby.agg`.
        '''
        if aggregate_kwargs is None:
            aggregate_kwargs = {}

        pipeline = []
        if pre_filter is not None:
            pipeline.append({"$match": pre_filter})

        if not field_name.startswith('$'):
            field_name = '$' + field_name
        group_stage = {'_id': field_name}
        pipeline.append({'$group': group_stage})

        if post_filter is not None:
            pipeline.append({"$match": post_filter})

        return MongoGroupby(
            self, pipeline, collections_subset=collections_subset, **aggregate_kwargs
        )

    def distinct(self, field_name: str) -> set:
        """Return a set with all the possible values that the field can take in the database.

        Example:

        >>> import querier
        >>> with querier.Connection('twitter_2020') as con:
        >>>     con.distinct('place.country')
        {'Spain', 'France', 'Portugal', 'Germany', ...}
            

        :param field_name: The name of the field to test
        :return: set of different values
        """
        module_logger.debug("######### Begin distinct('{}') #########".format(field_name))        
        module_logger.debug("dbname '{}' | process pid {}"\
            .format(self._dbname, getpid()))

        @_pymongo_call
        def internal_distinct(self, coll, field_name):
            return self._db[coll].distinct(field_name)

        result = set()
        for coll in self._db.list_collection_names():
            d = internal_distinct(self, coll, field_name)
            if d is not None:
                module_logger.debug("Executed distinct in "+coll)
                result = result.union(set(d))

        return result


    def _field_list_to_projection_dict(fields: list):
        """Convert a list of field names to a pymongo-formatted projection dictionary."""
        if fields is None or len(fields) == 0:
            return None

        proj = dict()
        for f in fields:
            proj[f] = 1
        return proj       


    def _test_connection(self) -> bool:
        base_message = "Error accessing the database '"\
            + self._dbname + "'."

        try:
            self._con.admin.command('ismaster')
            return True    
        
        except pymongo.errors.OperationFailure as e:
            if e.code == AUTHENTIFICATION_FAILED:
                message = base_message + " Credentials are wrong or the user "\
                    + "does not has sufficient permissions."
                raise AuthentificationError(message) from None

        except pymongo.errors.ServerSelectionTimeoutError:
            message = base_message + " No Mongo server is available "\
                    + " at the host or port found in the credentials file"
            raise ServerError(message) from None

        except pymongo.errors.ConnectionFailure:
            message = base_message + " The connection to the database failed"
            raise ServerError(message) from None
        
        return False


    def _get_config_option(config, main_section, option) -> str:
        if not config.has_option(main_section, option):
            raise CredentialsError(
                "Option '{}' in credentials file is missing".\
                format(option)) from None
            return None

        return config.get(main_section, option)


    def _find_section_from_suffixes(dbname, config):
        for section in config.sections():
            if config.has_option(section, 'suffixes'):
                suffixes = config.get(section, 'suffixes').split(',')

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
            raise CredentialsError("File '"+expanded_path+"' not found.") from None
        except OSError:
            raise CredentialsError("Error reading file '"+expanded_path+"'.") from None
        except configparser.Error as err:
            module_logger.error("Error parsing file: "+err)
            raise CredentialsError("Error parsing file '"+expanded_path+"'.") from None
        except Exception as err:
            module_logger.error("Unhandled exception reading '"+expanded_path+"': "+err)      


        if config.has_section(dbname):
            section_name = dbname
        else:
            section_name = Connection._find_section_from_suffixes(dbname, config)

        if section_name is None:
            raise CredentialsError(
                "Database name '{}' not found in the credentials file '{}'".\
                format(dbname, expanded_path))

        dbtype = Connection._get_config_option(config, section_name, "type")
        user = Connection._get_config_option(config, section_name, "ruser")
        pwd  = Connection._get_config_option(config, section_name, "rpwd")     

        if not config.has_section(dbtype):
            raise CredentialsError(
                "Section '{}' is missing in file '{}' (required for database '{}')".\
                format(dbtype, expanded_path, dbname))

        host = Connection._get_config_option(config, dbtype, "host") 
        port = Connection._get_config_option(config, dbtype, "port") 

        return host, port, user, pwd


    def _create_connection(self, dbname, credentials_path):
        host, port, user, pwd =\
            Connection._parse_credentials_file(credentials_path, dbname)

        # Connect to DB
        mongoserver_uri = "mongodb://{u}:{w}@{h}:{p}/{db}?authSource={db}"\
            .format(db=dbname, u=user, p=port, w=pwd, h=host)
        conection = pymongo.MongoClient(host=mongoserver_uri,
            replicaSet=None, connect=True)

        module_logger.info("Started connection to... ")
        module_logger.info("    * Database name: " + dbname)
        module_logger.info("    * Host: " + host)
        module_logger.info("    * Port: " + port)
        module_logger.info("    * PID: " + repr(getpid()))

        return conection, conection[dbname]


class MongoGroupby:
    def __init__(
        self,
        connection: Connection,
        pipeline: list,
        collections_subset: list | None = None,
        **agg_kwargs,
    ):
        self.connection = connection
        self.pipeline = pipeline
        self.collections_subset = collections_subset
        self.agg_kwargs = agg_kwargs

    def agg(self, **aggregations) -> Result:
        '''
        Works on the model of named aggregations of 
        :py:meth:`pandas.core.groupby.DataFrameGroupBy.aggregate`, except we
        provide a :py:meth:`querier.NamedAgg` with keywords `field` and
        `aggfunc`. For reference see
        https://pandas.pydata.org/pandas-docs/stable/user_guide/groupby.html?highlight=filter#named-aggregation
        '''
        group_stage = [stage for stage in self.pipeline if '$group' in stage][0]
                       
        for output_field, agg_descr in aggregations.items():
            if not hasattr(agg_descr, 'aggfunc'):
                agg_descr = NamedAgg(*agg_descr)
            aggfunc = agg_descr.aggfunc
            if not aggfunc.startswith('$'):
                aggfunc = '$' + aggfunc

            if aggfunc == '$count':
                aggfunc = '$sum'
                input_field = 1
            else:
                input_field = agg_descr.field
                if not input_field.startswith('$'):
                    input_field = '$' + input_field

            group_stage['$group'][output_field] = {aggfunc: input_field}

        return self.connection.aggregate(
            self.pipeline, collections_subset=self.collections_subset, **self.agg_kwargs
        )
