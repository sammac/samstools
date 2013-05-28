#!/usr/bin/env python

from sqlalchemy import create_engine, text

class SamsDBWrapper(object):
    """Simple SQLAlchemy wrapper 
    Designed for simple sql statements, using sqlalchemy's named parameters and
    returning an easily parsable list of dictionaries.
    """
    def __init__(self, db_type='', user='', password='', host='', database=''):
        """"Initialise engine and connect to database."""
        engine = create_engine("%s://%s:%s@%s/%s" % (db_type,
                                                     user,
                                                     password,
                                                     host,
                                                     database
                                                     ))
        self.connection = engine.connect()

    def _resultproxy_to_list_of_dicts(self, result):
        """Converts the ResultProxy recieved by the session to a list of 
        dictionaries."""
        r_list = []
        for row in result:
            row_as_dict = dict(row)
            r_list.append(row_as_dict)
        return r_list

    def _query(self, query, **kwargs):
        """Converts a text SQL expression to a sqlalchemy object, adding named 
        parameters and returning the result as a list of dicts."""
        query = text(query)
        result = self.connection.execute(query,kwargs)
        return self._resultproxy_to_list_of_dicts(result)

    def __del__(self):
        """Attempt to close connection on destruction."""
        try:
            self.connection.close()
        except:
            pass
