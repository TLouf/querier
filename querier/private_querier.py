"""Module docstring."""

import pymongo
import configparser
import time

import logging
import logging.handlers

from pprint import pprint, pformat
from os.path import expanduser
from os import getpid

from functools import wraps


module_logger = logging.getLogger("querier")

def init_logger(name, filename, level=logging.DEBUG):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    file_formatter = logging.Formatter('[%(levelname)s] <%(asctime)s>: %(message)s')

    max_size = 256 * 1024
    fh = logging.handlers.RotatingFileHandler(filename, maxBytes=max_size)
    fh.setFormatter(file_formatter)

    logger.addHandler(fh)
    return logger


# ---------------------- querier Errors ----------------------
UNAUTHORIZED_COMMAND = 13
AUTHENTIFICATION_FAILED = 18

class InvalidFilter(Exception):
    """Raised when an operation on a filter is invalid."""
    
    pass

class CredentialsError(Exception):
    """Raised when Connection constructor fails to read or parse the credentials file."""
    
    pass

class AuthentificationError(Exception):
    """Raised by Connection if the credentials are invalid."""
    
    pass

class ServerError(Exception):
    """Raised by Connection when any server-related error takes place during a connection."""
    
    pass

class InternalError(Exception):
    """Raised by when an internally unhandled exception ocurred."""

    pass


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

# -----------------------------------------------------------

class Filter:
    """Filter objects are used to compose search criteria for extracting data from a database.

    They are used in :py:class:`querier.Connection` methods to filter
    data from the database. A filter is composed by simple conditions.
    By default, an empty filter will match all the entries from the database.
    A condition is added by calling any condition method on the filter object. 

    For example, the following code::
        
        from querier import Filter
        f = Filter()
        f.greater_than('x', 0)
        f.less_or_equals('x', 100)

    Creates a filter that matches all entries with a field named 'x' whose value is in range (0, 100].
    As showed in the example, a call to a condition method performs an *AND* operation. 
    (To perform an *OR* operation, see :py:meth:`Filter.or_filter()`)

    .. note:: 
    
       Entries are collections of pairs field_id-value. They are returned as python 
       dictionaries and values can of any type: string, numeric, list, dictionary,...
       Example of an entry from the twitter database::

            {
                "lang": "es",
                "place": {
                    coordinates': [[[-109.479171, -56.557358],
                                    [-109.479171, -17.497384],
                                    [-66.15203, -17.497384],
                                    [-66.15203, -56.557358]]]
                    "country_code": "ES",
                    ...
                },
                "favorite_count": 124,
                ...
            }

       To filter by nested fields the dot ('.') notation should be used. For example, the following code:

            >>> f = Filter()
            >>> f.any_of('place.country_code', ['ES', 'FR', 'PT']) 

       will match entries from Spain (ES), France (FR) or Portugal (PT).    

    """

    def __init__(self):
        self._query = {}

    def __str__(self):
        return pformat(self._query)

    def __repr__(self):
        return self.__str__()

    def get_query(self):
        return self._query

    def exists(self, field_id):
        """Add a condition to the filter that matches when the field exists.

        **python equivalent:** field_id is not None
        """
        self._add_operation(field_id, "$exists", True)
        return self


    def not_exists(self, field_id):
        """Add a condition to the filter that matches when the field doesn't exists.

        **python equivalent:** field_id is None
        """
        self._add_operation(field_id, "$exists", False)
        return self


    def equals(self, field_id, value):
        """Add a condition to the filter that matches when the field is equals to a value.

        **python equivalent:** field_id == value
        """
        self._add_operation(field_id, "$eq", value)
        return self

    def not_equals(self, field_id, value):
        """Add a condition to the filter that matches when the field is not equals to a value.

        **python equivalent:** field_id != value
        """
        self._add_operation(field_id, "$ne", value)
        return self

    def any_of(self, field_id, values: list):
        """Add a condition to the filter that matches when the field is 
        equals to any of the values in the list.

        **python equivalent:** field_id in values
        """
        if type(values) is not list:
            raise InvalidFilter("values argument must be of type 'list'")

        self._add_operation(field_id, "$in", values)
        return self

    def none_of(self, field_id, values: list):
        """Add a condition to the filter that matches when the field is 
        not equals to any of the values in the list.

        **python equivalent:** field_id not in values
        """
        if type(values) is not list:
            raise InvalidFilter("values argument must be of type 'list'")

        self._add_operation(field_id, "$nin", values)
        return self

    def greater_than(self, field_id, value):
        """Add a condition to the filter that matches when the field is 
        greater than a value.

        **python equivalent:** field_id > value
        """
        self._add_operation(field_id, "$gt", value)
        return self

    def greater_or_equals(self, field_id, value):
        """Add a condition to the filter that matches when the field is
        greater or equals than a value.

        **python equivalent:** field_id >= value
        """
        self._add_operation(field_id, "$gte", value)
        return self

    def less_than(self, field_id, value):
        """Add a condition to the filter that matches when the field is
        less than a value.

        **python equivalent:** field_id < value
        """
        self._add_operation(field_id, "$lt", value)
        return self

    def less_or_equals(self, field_id, value):
        """Add a condition to the filter that matches when the field is
        less or equals than a value.

        **python equivalent:** field_id <= value
        """
        self._add_operation(field_id, "$lte", value)
        return self

    def is_empty(self):
        """Return True if the filter is empty (has no conditions), False otherwise."""
        return len(self._query.keys()) == 0
        return self

    def or_filter(self, other):
        """Perform an OR operation with a second filter.

        Example::

            import querier

            f1 = querier.Filter()
            f1.greater_or_equals('retweet_count', 1000)

            f2 = querier.Filter()
            f2.greater_or_equals('favorite_count', 500)

            f1.or_condition(f2)

        f1 is a filter that matches tweets with a number of retweets 
        larger than 1000 OR a number of favorites larger than 500

        :param other: a filter object
        :type other: :py:class:`querier.Filter`
        :raises InvalidFilter: if one of the filters is empty or both filters are the same object
        :return: self
        :rtype: :py:class:`querier.Filter`
        """
        if other.is_empty() or self.is_empty():
            raise InvalidFilter("One of the filters is empty")

        if other == self:
            raise InvalidFilter("The argument cannot be the caller filter")

        cond1 = self._query
        cond2 = other._query
        self._query = {"$or": [cond1, cond2]}
        return self

    def _add_operation(self, field_id, operation, value):
        if type(field_id) is not str:
            raise InvalidFilter("field_id argument must be a string")

        if field_id not in self._query:
            self._query[field_id] = {}
        target = self._query[field_id]
        
        target[operation] = value
        return self

class Result:
    """Multi value result returned by the :py:class:`querier.Connection` class.

    To iterate through the entries a use a python for-loop::

        with querier.Connection(database_name, credentials_file) as con:
            result = con.extract(f)                
            for entry in result:
                # Process entry (type: dict)

    .. warning:: If the Connection object used to extract the Result is closed, the result
        object becomes invalid and will return 0 entries::
            
            import querier

            with querier.Connection(database_name, credentials_file) as con:
                result = con.extract(f)                

            for entry in result:
                # Execution will never enter this loop because the Connection objet was closed
                # by the 'with' keyword

        

    """
    def __init__(self):
        self._cursors = []
        self._collection_names = []
        self._idx = 0
        self._docs_returned = 0
        self._limit = 0

    def _add_cursors(self, cursors, names=None):
        self._cursors += cursors
        self._collection_names += names

    def __iter__(self):
        self._idx = 0
        self._docs_returned = 0
        module_logger.debug("Start result loop ({} cursors)".format(len(self._cursors)))
        return self

    def limit(self, qty):
        """Limits the number of entries for this result.

        If qty is:
        
            * <= 0, the result will return all the entries that matched the filter.
            * > 0, the result will return max(qty, number of entries that matched) entries

        Example::

            f = Filter()

            # Limits the extraction result to 100 entries
            result = con.extract(f).limit(100)
            
            for entry in result:
                # Process entry

        This snippet will iterate between 0 and 100 entries (depending how many entries matched the filter)

        :param qty: max number of entries
        :type qty: int
        :return: self
        :rtype: :py:class:`querier.Result`
        """
        self._limit = max(0, qty)
        return self

    def __next__(self):
        # Stops recursion when there are no more 
        # cursors available
        if self._idx >= len(self._cursors) or\
            (self._docs_returned >= self._limit and\
            self._limit > 0):
            module_logger.debug("Result finsished")
            raise StopIteration
        
        # Try to get next document from the current cursor.
        # If cursor raises StopIteration:
        #   then get next document from the next cursor 
        #   until there are no more cursors or some
        #   cursor returns a document
        try:
            current_cursor = self._cursors[self._idx]
            next_doc = next(current_cursor)
            self._docs_returned += 1
            return next_doc
        except StopIteration:
            module_logger.debug("End of cursor from " + self._collection_names[self._idx])
            # Try to get documents from the next cursor instead
            self._idx += 1
            return self.__next__()

        except pymongo.errors.OperationFailure as e:
            # A collection can deny querying to that user (system.profile)
            if e.code != UNAUTHORIZED_COMMAND:
                module_logger.error("Unhandled exception in result: "+err)
                raise e
            else:
                # Try to get documents from the next cursor instead
                self._idx += 1
                return self.__next__()


    def close(self):
        """Closes the result

        The result becomes invalid and won't return any entries when
        iterated.
        """
        module_logger.debug("Closing Result ({} cursors)".format(len(self._cursors)))
        for cursor in self._cursors:
            cursor.close()
        self._cursors.clear()

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

    def __init__(self, dbnamecfg, credentials_path="~/.credentials.cfg"):
        """Create and initiate a connection to a database.

        :param dbnamecfg: name of the database
        :type dbnamecfg: str
        :param credentials_path: path to a configuration file with the credentials
        to read the database
        :type credentials_path: str
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


    def count_entries(self, filter=None, collection=None) -> int:
        """Count the entries that matches a filter.

        Example: Count how many tweets from Spain in 2014 have more than 500 favorites

        >>> from querier import *
        >>> f = Filter()
        >>> f.greater_than('favorite_count', 500)
        >>> with Connection('twitter_2014') as con:
        >>>     count = con.count_entries(f, collection='spain')
        3

        :param filter: Filter object to count entries or None (all entries)
        :type filter: :py:class:`querier.Filter`
        :param collection: A collection name from :py:meth:`Connection.list_available_collections()` or None (all collections)
        :type collection: str
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


    def extract_one(self, filter=None, collections_subset: list = None) -> dict:
        """Extract an entry from the database that matches a filter.

        :param filter: filter that will be used to test the entries
        :type filter: :py:class:`querier.Filter`
        :param collections_subset: list of collections to extract from. A subset of :py:meth:`Connection.list_list_available_collections()`
        :type collections_subset: list
        :return: entry from the database or None if no entry matches the filter
        :rtype: dict
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
    def list_available_collections(self):
        """Return a list of available collections.

        MongoDB databases can be splitted by collections. For example, all tweets from USA are
        in collection 'northern_america' in twitter databases. Contact the database administrator 
        to know how/if collections are semantically splitted.

        Extraction methods can be sped up by using a subset of collections.

        :return: a list of all the available collection names
        :rtype: str
        """
        names = self._db.list_collection_names()
        for n in names:
            if 'system' in n:
                names.remove(n)
        return names


    def extract(self, filter = None, fields: list = None, collections_subset: list = None):
        """Extract entries from the database that matches a filter.

        To limit the number of entries that will be returned, use :py:meth:`Result.limit()`. As 
        databases can contain a huge number of entries, it is advised to test the code 
        with a limited result first.

        To iterate through the entries, see :py:class:`querier.Result`

        :param filter: filter to test the entries
        :type filter: :py:class:`querier.Filter`
        :param fields: List of selected fields from the original database that the result dictionaries 
                       will have. This is useful when only a subset of the fields is needed.
        :type fields: list
        :param collections_subset: list of collections to extract from. A subset of :py:meth:`Connection.list_list_available_collections()`
        :type collections_subset: list
        :return: entry from the database or None if no entry matches the filter
        :rtype: :py:class:`querier.Result`
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


    def distinct(self, field_name: str):
        """Return a set with all the possible values that the field can take in the database.

        Example:
        >>> import querier
        >>> with querier.Connection('twitter_2020') as con:
        >>>     con.distinct('place.country')
        {'Spain', 'France', 'Portugal', 'Germany', ...}
            

        :param field_name: The name of the field to test
        :type field_name: string
        :return: set of different values
        :rtype: set
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
