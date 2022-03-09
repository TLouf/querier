"""Module docstring."""
from __future__ import annotations

from pprint import pformat
from typing import TYPE_CHECKING, Mapping, Union

import bson

from querier.exceptions import InvalidFilter

if TYPE_CHECKING:
    import re

    from shapely.geometry import MultiPolygon, Polygon

    GeoFilterType = Union[Polygon, MultiPolygon, list]


class Filter(dict):
    """Dict subclass to compose search criteria for extracting data from a database.

    They are used in :py:class:`querier.Connection` methods to filter
    data from the database. A filter is composed by simple conditions.
    By default, an empty filter will match all the entries from the database.
    A condition is added by calling any condition method on the filter object.

    For example, the following code::

        import querier as qr
        f = qr.Filter()
        f.greater_than('x', 0)
        f.less_or_equals('x', 100)

    Creates a filter that matches all entries with a field named 'x' whose value is in
    range (0, 100]. As showed in the example, a call to a condition method performs an
    *AND* operation.
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

       To filter by nested fields the dot ('.') notation should be used. For example,
       the following code::

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
        return self._logical_op("$or", other)

    def __and__(self, other):
        return self._logical_op("$and", other)

    def copy(self) -> Filter:
        return Filter(super().copy())

    def get_query(self) -> Filter:
        return self

    def exists(self, field_id: str) -> Filter:
        """Add a condition to the filter that matches when the field exist.

        **python equivalent:** ``field_id is not None``
        """
        self._add_operation(field_id, "$exists", True)
        return self

    def not_exists(self, field_id: str) -> Filter:
        """Add a condition to the filter that matches when the field doesn't exist.

        **python equivalent:** ``field_id is None``
        """
        self._add_operation(field_id, "$exists", False)
        return self

    def equals(self, field_id, value: str) -> Filter:
        """
        Add a condition to the filter that matches when the field is equal to a value.

        **python equivalent:** ``field_id == value``
        """
        self._add_operation(field_id, "$eq", value)
        return self

    def not_equals(self, field_id: str, value) -> Filter:
        """
        Add a condition to the filter that matches when the field is not equal to a
        value.

        **python equivalent:** ``field_id != value``
        """
        self._add_operation(field_id, "$ne", value)
        return self

    def any_of(self, field_id, values: list) -> Filter:
        """
        Add a condition to the filter that matches when the field is equal to any of the
        values in the list.

        **python equivalent:** ``field_id in values``
        """
        if not isinstance(values, list):
            raise InvalidFilter("values argument must be of type 'list'")

        self._add_operation(field_id, "$in", values)
        return self

    def none_of(self, field_id: str, values: list) -> Filter:
        """
        Add a condition to the filter that matches when the field is not equal to any of
        the values in the list.

        **python equivalent:** ``field_id not in values``
        """
        if not isinstance(values, list):
            raise InvalidFilter("values argument must be of type 'list'")

        self._add_operation(field_id, "$nin", values)
        return self

    def greater_than(self, field_id: str, value) -> Filter:
        """
        Add a condition to the filter that matches when the field is greater than a
        value.

        **python equivalent:** ``field_id > value``
        """
        self._add_operation(field_id, "$gt", value)
        return self

    def greater_or_equals(self, field_id: str, value) -> Filter:
        """
        Add a condition to the filter that matches when the field is greater or equal
        to a value.

        **python equivalent:** ``field_id >= value``
        """
        self._add_operation(field_id, "$gte", value)
        return self

    def less_than(self, field_id, value) -> Filter:
        """
        Add a condition to the filter that matches when the field is less than a value.

        **python equivalent:** ``field_id < value``
        """
        self._add_operation(field_id, "$lt", value)
        return self

    def less_or_equals(self, field_id, value) -> Filter:
        """
        Add a condition to the filter that matches when the field is less or equal to
        a value.

        **python equivalent:** ``field_id <= value``
        """
        self._add_operation(field_id, "$lte", value)
        return self

    def regex(
        self, field_id: str, pattern: str | re.Pattern | bson.regex.Regex
    ) -> Filter:
        """
        Add a condition to the filter that matches when the field matches the regular
        expression `pattern`.

        **python equivalent:** ``pattern.match(field_id) is not None``
        """
        if isinstance(pattern, str):
            pattern = bson.regex.Regex(pattern)
        self._add_operation(field_id, "$regex", pattern)
        return self

    def geo_within(
        self,
        field_id: str,
        geo: GeoFilterType,
        geo_type: str = "Polygon",
        invert: bool = False,
    ) -> Filter:
        """
        Add a condition to the filter that matches when the field's geometry is fully
        contained within a geometry `geo`.

        Parameters:
            field_id: Name of the field supposed to contain geometries.
            geo:
                Geometry within which they have to be to pass the filter. Can be given
                as a shapely geometry or as nested lists giving the coordinates of the
                geometry. For ``geo_type="bbox"``, should be
                ``[minx, miny, maxx, maxy]``.
            geo_type:
                Type of the geometry: "Polygon", "MultiPolygon" or "bbox". Does not need
                to be specified when `geo` is a shapely geometry.
            invert:
                If True, filters out geometries that are within `geo`, if False
                (default), keeps only those that are within `geo`.

        Warnings:
            Geometries in MongoDB use ``EPSG:4326`` as the default coordinate reference
            system (CRS). Beware also that the geometries in the field `field_id` should
            be valid, so for instance polygons should be closed. This is not the case
            for ``place.bounding_box`` in tweet collections, and only the case for
            ``valid_bounding_box`` in places collections.
        """
        return self._geo_op("$geoWithin", field_id, geo, geo_type, invert)

    def geo_intersects(
        self,
        field_id: str,
        geo: GeoFilterType,
        geo_type: str = "Polygon",
        invert: bool = False,
    ) -> Filter:
        """
        Add a condition to the filter that matches when the field's geometry intersects
        with a geometry `geo`.

        Parameters:
            field_id: Name of the field supposed to contain geometries.
            geo:
                Geometry that has to be intersected to pass the filter. Can be given as
                a shapely geometry or as nested lists giving the coordinates of the
                geometry. For ``geo_type="bbox"``, should be
                ``[minx, miny, maxx, maxy]``.
            geo_type:
                Type of the geometry: "Polygon", "MultiPolygon" or "bbox". Does not need
                to be specified when `geo` is a shapely geometry.
            invert:
                If True, filters out geometries that intersect `geo`, if False
                (default), keeps only those that intersect `geo`.

        Warnings:
            Geometries in MongoDB use ``EPSG:4326`` as the default coordinate reference
            system (CRS). Beware also that the geometries in the field `field_id` should
            be valid, so for instance polygons should be closed. This is not the case
            for ``place.bounding_box`` in tweet collections, and only the case for
            ``valid_bounding_box`` in places collections.
        """
        return self._geo_op("$geoIntersects", field_id, geo, geo_type, invert)

    def _geo_op(
        self, op: str, field_id: str, geo: GeoFilterType, geo_type: str, invert: bool
    ):
        """
        Invert only implemented for `geo_op` because only place it's useful (for now).
        """
        if geo_type.endswith("Polygon"):
            try:
                import shapely.geometry as shplygeo

                if not isinstance(geo, (shplygeo.Polygon, shplygeo.MultiPolygon)):
                    geo = getattr(shplygeo, geo_type)(geo)
                geo_dict = shplygeo.mapping(geo)

            except ImportError:
                # Close the polygon if not done
                if geo[0] != geo[-1]:
                    geo.append(geo[0])
                geo_dict = {"coordinates": [geo], "type": geo_type}

        # If geo type is box or bbox or bounding_box...
        elif geo_type.lower().endswith("box"):
            # `geo` assumed in format (min_x, min_y, max_x, max_y), as the
            # return value of GeoDataFrame.total_bounds.
            minx, miny, maxx, maxy = geo
            geo_dict = {
                "coordinates": [
                    [
                        [minx, miny],
                        [minx, maxy],
                        [maxx, maxy],
                        [maxx, miny],
                        [minx, miny],
                    ]
                ],
                "type": "Polygon",
            }

        else:
            raise ValueError('geo_type must either be "(Multi)Polygon" or "bbox"')

        value = {"$geometry": geo_dict}
        self._add_operation(field_id, op, value, invert=invert)
        return self

    def is_empty(self) -> bool:
        """Return True if the filter is empty (has no conditions), False otherwise."""
        return len(self) == 0

    def or_filter(self, other: Filter | dict) -> Filter:
        """Perform an OR operation with a second filter, modifies inplace.

        Parameters:
            other: Another Filter object or dict to perform the operation with.

        Returns:
            self

        Raises:
            InvalidFilter: if one of the filters is empty or both filters are the same
                object.

        Examples:
            Create a Filter f1 that matches tweets with a number of retweets
            larger than 1000 OR a number of favorites larger than 500::

                import querier as qr

                f1 = qr.Filter()
                f1.greater_or_equals('retweet_count', 1000)

                f2 = qr.Filter()
                f2.greater_or_equals('favorite_count', 500)

                f1.or_filter(f2)

            Alternative notation using the ``|`` operator, not modifying `f1` in place
            but getting the resulting Filter as a new instance `f3`::

                f3 = f1 | f2
        """
        self = self | other
        return self

    def and_filter(self, other: Filter | dict) -> Filter:
        """Perform an AND operation with a second filter, modifies inplace.

        Parameters:
            other: Another Filter object or dict to perform the operation with.

        Returns:
            self

        Raises:
            InvalidFilter: if one of the filters is empty or both filters are the same
                object

        Examples:
            Create a Filter f1 that matches tweets with a number of retweets between
            500 and 1000::

                import querier as qr

                f1 = qr.Filter()
                f1.greater_or_equals('retweet_count', 500)

                f2 = qr.Filter()
                f2.less_or_equals('retweet_count', 1000)

                f1.and_filter(f2)

            Alternative notation using the ``&`` operator, not modifying `f1` in place
            but getting the resulting Filter as a new instance `f3`::

                f3 = f1 & f2
        """
        self = self & other
        return self

    def _logical_op(self, op: str, other: Mapping) -> Filter:
        if self.is_empty() or len(other) == 0:
            raise InvalidFilter("One of the filters is empty")

        if other == self:
            raise InvalidFilter("The argument cannot be the caller filter")

        if list(self.keys()) == [op]:
            res = self.copy()
            # to chain operations
            res[op].append(other)
        else:
            conditions = [self.copy(), other]
            res = Filter({op: conditions})
        return res

    def _add_operation(self, field_id, operation, value, invert=False):
        if not isinstance(field_id, str):
            raise InvalidFilter("field_id argument must be a string")

        if field_id not in self:
            self[field_id] = {}
        target = self[field_id]

        if invert:
            if "$not" not in target:
                target["$not"] = {}
            target["$not"][operation] = value
        else:
            target[operation] = value
        return self
