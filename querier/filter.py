"""Module docstring."""
from __future__ import annotations

from typing import TYPE_CHECKING
from pprint import pformat

from .exceptions import InvalidFilter

if TYPE_CHECKING:
    import re
    from shapely.geometry import Polygon, MultiPolygon
    GeoFilterType = Polygon | MultiPolygon | list


class Filter(dict):
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
       Excerpt of an example entry from the twitter database:

       .. code-block:: json

            {
                "lang": "es",
                "place": {
                    "coordinates": [[[-109.479171, -56.557358],
                                    [-109.479171, -17.497384],
                                    [-66.15203, -17.497384],
                                    [-66.15203, -56.557358]]]
                    "country_code": "ES",
                },
                "favorite_count": 124,
            }

       To filter by nested fields the dot ('.') notation should be used. For example, the following code::

            f = Filter()
            f.any_of('place.country_code', ['ES', 'FR', 'PT']) 

       will match entries from Spain (ES), France (FR) or Portugal (PT).    

    """

    def __str__(self):
        # super().copy() to avoid RecursionError
        return pformat(super().copy())

    def __repr__(self):
        return str(self)

    def __or__(self, other):
        return self.or_filter(other)

    def __and__(self, other):
        return self.and_filter(other)
        
    def copy(self):
        return Filter(super().copy())

    def get_query(self):
        return self

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

    def regex(self, field_id: str, pattern: str | re.Pattern):
        """Add a condition to the filter that matches when the field matches the
        regular expression `pattern`.

        **python equivalent:** pattern.match(field_id) is not None
        """
        self._add_operation(field_id, "$regex", pattern)
        return self

    def geo_within(self, field_id: str, geo: GeoFilterType, geo_type: str = 'Polygon'):
        """Add a condition to the filter that matches when the field's geometry
        is fully contained within a geometry `geo`.
        """
        return self._geo_op('$geoWithin', field_id, geo, geo_type)

    def geo_intersects(self, field_id: str, geo: GeoFilterType, geo_type: str = 'Polygon'):
        """Add a condition to the filter that matches when the field's geometry
        intersects with a geometry `geo`.
        """
        return self._geo_op('$geoIntersects', field_id, geo, geo_type)

    def _geo_op(self, op: str, field_id: str, geo: GeoFilterType, geo_type: str):
        if geo_type.endswith('Polygon'):
            try:
                import shapely.geometry as shplygeo
                if not isinstance(geo, (shplygeo.Polygon, shplygeo.MultiPolygon)):
                    geo = getattr(shplygeo, geo_type)(geo)
                geo_dict = shplygeo.mapping(geo)

            except ImportError:
                # Close the polygon if not done
                if geo[0] != geo[-1]:
                    geo.append(geo[0])
                geo_dict = {
                    'coordinates': [geo],
                    'type': geo_type
                }

            value = {'$geometry': geo_dict}

        # If geo type is box or bbox or bounding_box...
        elif geo_type.lower().endswith('box'):
            # `geo` assumed in format (min_x, min_y, max_x, max_y), as the
            # return value of GeoDataFrame.total_bounds. 'box' only for field
            # containing legacy coordinates, that is not geojson but arrays of
            # coordinates or stuff like that 
            value = {'$box': [[geo[0], geo[1]], [geo[2], geo[3]]]}

        else:
            raise ValueError('geo_type must either be "(Multi)Polygon" or "bbox"')

        self._add_operation(field_id, op, value)

    def is_empty(self):
        """Return True if the filter is empty (has no conditions), False otherwise."""
        return len(self) == 0

    def or_filter(self, other):
        """Perform an OR operation with a second filter.

        Example::

            import querier

            f1 = querier.Filter()
            f1.greater_or_equals('retweet_count', 1000)

            f2 = querier.Filter()
            f2.greater_or_equals('favorite_count', 500)

            f1.or_filter(f2)

        f1 is a filter that matches tweets with a number of retweets 
        larger than 1000 OR a number of favorites larger than 500

        :param other: a filter object or dict
        :type other: :py:class:`querier.Filter` or :py:class:`dict`
        :raises InvalidFilter: if one of the filters is empty or both filters are the same object
        :return: self
        :rtype: :py:class:`querier.Filter`
        """
        return self._logical_op("$or", other)

    def and_filter(self, other):
        """Perform an AND operation with a second filter.

        Example::

            import querier

            f1 = querier.Filter()
            f1.greater_or_equals('retweet_count', 500)

            f2 = querier.Filter()
            f2.less_or_equals('retweet_count', 1000)

            f1.and_filter(f2)

        f1 is a filter that matches tweets with a number of retweets 
        between 500 and 1000.

        :param other: a filter object or dict
        :type other: :py:class:`querier.Filter` or :py:class:`dict`
        :raises InvalidFilter: if one of the filters is empty or both filters are the same object
        :return: self
        :rtype: :py:class:`querier.Filter`
        """
        return self._logical_op("$and", other)

    def _logical_op(self, op, other):
        if self.is_empty() or len(other) == 0:
            raise InvalidFilter("One of the filters is empty")

        if other == self:
            raise InvalidFilter("The argument cannot be the caller filter")

        if list(self.keys()) == [op]:
            # to chain operations
            self[op].append(other)
        else:
            conditions = [self.copy(), other]
            self.clear()
            self[op] = conditions
        return self

    def _add_operation(self, field_id, operation, value):
        if not isinstance(field_id, str):
            raise InvalidFilter("field_id argument must be a string")

        if field_id not in self:
            self[field_id] = {}
        target = self[field_id]
        
        target[operation] = value
        return self
