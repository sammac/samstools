#!/usr/bin/env python

from sqlalchemy import create_engine as sa_create_engine, text as sa_text

class DBWrapper(object):
    def __init__(self, db_type='', user='', password='', host='', database=''):
        """"Initialise engine and connect to database."""
        self._connect(db_type, user, password, host, database)

    def _connect(self,db_type, user, password, host, database):
        engine = sa_create_engine("%s://%s:%s@%s/%s" % (db_type,
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
        query = sa_text(query)
        result = self.connection.execute(query,kwargs)
        return self._resultproxy_to_list_of_dicts(result)

    def _disconnect(self):
        """Close connection."""
        self.connection.close()   

    def __del__(self):
        """Attempt to close connection on destruction."""
        try:
            self._disconnect()
        except:
            pass