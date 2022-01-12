"""Module docstring."""

from pprint import pformat

from .exceptions import InvalidFilter


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
