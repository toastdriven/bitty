# -*- coding: utf-8 -*-
"""
A tiny database layer.

Why another database layer? I wanted one that was small (both in terms of a
single file and in actual kloc), tested and could handle multiple data stores.
And because it was fun.

Example::

    from bitty import *
    
    bit = Bitty('sqlite:///home/code/my_database.db')
    
    bitty.add('people', name='Claris', says='Moof!', age=37)
    bitty.add('people', name='John Doe', says='No comment.', age=37)
    
    # Select all.
    for row in bitty.find('people'):
        print row['name']

You're responsible for your own schema. bitty does the smallest amount of
introspection it can to get by. bitty supports the usual CRUD method.

Tastes great when used with itty. Serious Python Programmers™ with Enterprise
Requirements need not apply.
"""
import re


__author__ = 'Daniel Lindsley'
__version__ = ('0', '1')


FILESYSTEM_DSN = re.compile(r'^(?P<adapter>\w+)://(?P<path>.*)$')
# DRL_FIXME - Make sure this is standardish.
# DRL_FIXME: Handle port?
DAEMON_DSN = re.compile(r'^(?P<adapter>\w+)://(?P<user>[\w\d_.-]+):(?P<pass>[\w\d_.-]+)@(?P<host>.*)$')


class BittyError(Exception): pass
class QueryError(BittyError): pass
class InvalidDSN(BittyError): pass


class BaseSQLAdapter(object):
    def __init__(self, dsn):
        self.connection = self.get_connection(dsn)
        self._tables = {}
    
    def get_connection(self, dsn):
        raise NotImplementedError("Subclasses must implement the 'get_connection' method.")
    
    def raw(self, query, params=[]):
        cursor = self.connection.cursor()
        return cursor.execute(query, params)
    
    def _get_column_names(self, **kwargs):
        raise NotImplementedError("Subclasses must implement the '_get_column_names' method.")
    
    def _build_insert_query(self, table, **kwargs):
        column_names = sorted(kwargs.keys())
        values = [kwargs[name] for name in column_names]
        binds = ['?' for value in values]
        query = "INSERT INTO %s (%s) VALUES (%s)" % (table, ', '.join(column_names), ', '.join(binds))
        return query, values
    
    def _build_update_query(self, table, pk, **kwargs):
        column_names = sorted(kwargs.keys())
        values = [kwargs[name] for name in column_names]
        # Add on the pk.
        values.append(pk)
        where = ["%s = ?" % name for name in column_names]
        query = "UPDATE %s SET %s WHERE id = ?" % (table, ' AND '.join(where))
        return query, values
    
    def _build_delete_query(self, table, pk):
        query = "DELETE FROM %s WHERE id = ?" % table
        return query, [pk]
    
    def _build_select_query(self, table, **kwargs):
        all_column_names = self._get_column_names(table)
        where_column_names = sorted(kwargs.keys())
        values = [kwargs[name] for name in where_column_names]
        where = ["%s = ?" % name for name in where_column_names]
        query = "SELECT %s FROM %s" % (', '.join(all_column_names), table)
        
        if len(kwargs):
            query = "%s WHERE %s" % (query, ' AND '.join(where))
        
        return query, values
    
    def add(self, table, **kwargs):
        if not len(kwargs):
            raise QueryError("The 'add' method requires at least one pair of kwargs.")
        
        query, values = self._build_insert_query(table, **kwargs)
        result = self.raw(query, params=values)
        return result.rowcount == 1
    
    def update(self, table, pk, **kwargs):
        query, values = self._build_update_query(table, pk, **kwargs)
        result = self.raw(query, params=values)
        return result.rowcount == 1
    
    def delete(self, table, pk):
        query, values = self._build_delete_query(table, pk)
        result = self.raw(query, params=values)
        return result.rowcount == 1
    
    def find(self, table, **kwargs):
        query, values = self._build_select_query(table, **kwargs)
        result = self.raw(query, params=values)
        rows = []
        column_names = self._get_column_names(table)
        
        for row in result.fetchall():
            row_info = {}
            
            for count, column in enumerate(row):
                row_info[column_names[count]] = column
            
            rows.append(row_info)
        
        return rows


class SQLiteAdapter(BaseSQLAdapter):
    def get_connection(self, dsn):
        match = FILESYSTEM_DSN.match(dsn)
        
        if not match:
            raise InvalidDSN("'sqlite' adapter received an invalid DSN '%s'." % dsn)
        
        details = match.groupdict()
        
        import sqlite3
        return sqlite3.connect(details['path'])
    
    def _get_column_names(self, table):
        if not table in self._tables:
            result = self.raw("SELECT * FROM %s" % table)
            self._tables[table] = sorted([column[0] for column in result.description])
        
        return self._tables[table]


class Bitty(object):
    ADAPTERS = {
        'sqlite': SQLiteAdapter,
        # 'json': {'module': 'json', 'dsn_regex': FILESYSTEM_DSN},
        # 'mysql': {'module': 'mysqldb', 'dsn_regex': DAEMON_DSN},
        # 'postgres': {'module': 'postgres_pyscopg2', 'dsn_regex': DAEMON_DSN},
    }
    
    def __init__(self, dsn):
        """
        Valid DSNs::
        
            * sqlite:///Users/daniellindsley/test.db
            * postgres://daniel:my_p4ss@localhost:5432
        """
        self.dsn = dsn
        self.adapter = self.get_adapter()
    
    def get_adapter(self, dsn=None):
        if dsn is None:
            dsn = self.dsn
        
        adapter_name = None
        
        for name in self.ADAPTERS:
            if dsn.startswith(name):
                adapter_name = name
        
        if adapter_name is None:
            raise InvalidDSN("'%s' is not a recognizable DSN." % dsn)
        
        adapter_klass = self.ADAPTERS[adapter_name]
        return adapter_klass(dsn)
    
    def add(self, table, **kwargs):
        return self.adapter.add(table, **kwargs)
    
    def update(self, table, pk, **kwargs):
        return self.adapter.update(table, pk, **kwargs)
    
    def delete(self, table, pk):
        return self.adapter.delete(table, pk)
    
    def find(self, table, **kwargs):
        return self.adapter.find(table, **kwargs)
    
    def get(self, table, **kwargs):
        results = self.find(table, **kwargs)
        
        if len(results) == 0:
            return None
        
        return results[0]
    
    def raw(self, query, **kwargs):
        return self.adapter.raw(query, **kwargs)
