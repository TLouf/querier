"""Module docstring."""

import logging

import pymongo


module_logger = logging.getLogger("querier")
UNAUTHORIZED_COMMAND = 13


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

        except pymongo.errors.OperationFailure as err:
            # A collection can deny querying to that user (system.profile)
            if err.code != UNAUTHORIZED_COMMAND:
                module_logger.error("Unhandled exception in result: "+err)
                raise err
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
