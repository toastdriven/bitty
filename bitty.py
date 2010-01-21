# -*- coding: utf-8 -*-
"""
A tiny database layer.

Why another database layer? I wanted one that was small (both in terms of a
single file and in actual kloc), tested and could handle multiple data stores.
And because it was fun.

Example::

    from bitty import *
    
    bit = Bitty('sqlite:///home/code/my_database.db')
    
    bit.add('people', name='Claris', says='Moof!', age=37)
    bit.add('people', name='John Doe', says='No comment.', age=37)
    
    # Select all.
    for row in bit.find('people'):
        print row['name']
    
    bit.close()

You're responsible for your own schema. bitty does the smallest amount of
introspection it can to get by. bitty supports the usual CRUD methods.

Tastes great when used with itty. Serious Python Programmers™ with Enterprise
Requirements need not apply.
"""
import re


__author__ = 'Daniel Lindsley'
__version__ = ('0', '4', '1')


FILESYSTEM_DSN = re.compile(r'^(?P<adapter>\w+)://(?P<path>.*)$')
DAEMON_DSN = re.compile(r'^(?P<adapter>\w+)://(?P<user>[\w\d_.-]+):(?P<password>[\w\d_.-]*?)@(?P<host>.*?):(?P<port>\d*?)/(?P<database>.*?)$')


class BittyError(Exception): pass
class QueryError(BittyError): pass
class InvalidDSN(BittyError): pass


class BaseSQLAdapter(object):
    BINDING_OP = '%s'
    FILTER_OPTIONS = {
        'lt': "%s < %s",
        'lte': "%s <= %s",
        'gt': "%s > %s",
        'gte': "%s >= %s",
        'startswith': "%s LIKE %s",
        'endswith': "%s LIKE %s",
        'contains': "%s LIKE %s",
    }
    
    def __init__(self, dsn):
        self.connection = self.get_connection(dsn)
        self._tables = {}
    
    def get_connection(self, dsn):
        raise NotImplementedError("Subclasses must implement the 'get_connection' method.")
    
    def raw(self, query, params=[], commit=True):
        cursor = self.connection.cursor()
        
        try:
            cursor.execute(query, params)
            
            if commit:
                self.connection.commit()
        except:
            self.connection.rollback()
            raise
        
        return cursor
    
    def _get_column_names(self, **kwargs):
        raise NotImplementedError("Subclasses must implement the '_get_column_names' method.")
    
    def _build_insert_query(self, table, **kwargs):
        column_names = sorted(kwargs.keys())
        values = [kwargs[name] for name in column_names]
        binds = [self.BINDING_OP for value in values]
        query = "INSERT INTO %s (%s) VALUES (%s)" % (table, ', '.join(column_names), ', '.join(binds))
        return query, values
    
    def _build_where_clause(self, **kwargs):
        if len(kwargs) == 0:
            return '', []
        
        clauses = []
        bind_params = []
        
        keys = sorted(kwargs.keys())
        
        for column_spec in keys:
            value = kwargs[column_spec]
            column_info = column_spec.split('__')
            
            if len(column_info) > 2:
                raise QueryError("'%s' is not a supported lookup. Only one set of '__' is allowed." % column_spec)
            
            if len(column_info) == 1:
                clauses.append("%s = %s" % (column_info[0], self.BINDING_OP))
                bind_params.append(value)
            else:
                if column_info[1] == 'in':
                    placeholders = [self.BINDING_OP for val in value]
                    clauses.append("%s IN (%s)" % (column_info[0], ', '.join(placeholders)))
                    bind_params.extend([val for val in value])
                elif column_info[1] in self.FILTER_OPTIONS:
                    clauses.append(self.FILTER_OPTIONS[column_info[1]] % (column_info[0], self.BINDING_OP))
                    
                    if column_info[1] in ('startswith', 'contains'):
                        value = "%s%%" % value
                    
                    if column_info[1] in ('endswith', 'contains'):
                        value = "%%%s" % value
                    
                    bind_params.append(value)
                else:
                    # Assume an exact lookup.
                    clauses.append("%s = %s" % (column_info[0], self.BINDING_OP))
                    bind_params.append(value)
        
        final_clause = "WHERE %s" % ' AND '.join(clauses)
        return final_clause, bind_params
    
    def _build_update_query(self, table, pk, **kwargs):
        column_names = sorted(kwargs.keys())
        values = [kwargs[name] for name in column_names]
        # Add on the pk.
        values.append(pk)
        where = ["%s = %s" % (name, self.BINDING_OP) for name in column_names]
        query = "UPDATE %s SET %s WHERE id = %s" % (table, ', '.join(where), self.BINDING_OP)
        return query, values
    
    def _build_delete_query(self, table, pk):
        query = "DELETE FROM %s WHERE id = %s" % (table, self.BINDING_OP)
        return query, [pk]
    
    def _build_select_query(self, table, **kwargs):
        all_column_names = self._get_column_names(table)
        where_clause, where_values = self._build_where_clause(**kwargs)
        query = "SELECT %s FROM %s" % (', '.join(all_column_names), table)
        
        if len(kwargs):
            query = "%s %s" % (query, where_clause)
        
        return query, where_values
    
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
        result = self.raw(query, params=values, commit=False)
        rows = []
        column_names = self._get_column_names(table)
        
        for row in result.fetchall():
            row_info = {}
            
            for count, column in enumerate(row):
                row_info[column_names[count]] = column
            
            rows.append(row_info)
        
        return rows
    
    def close(self, commit=True):
        if commit:
            self.connection.commit()
        
        return self.connection.close()


class SQLiteAdapter(BaseSQLAdapter):
    BINDING_OP = '?'
    FILTER_OPTIONS = {
        'lt': "%s < %s",
        'lte': "%s <= %s",
        'gt': "%s > %s",
        'gte': "%s >= %s",
        'startswith': "%s LIKE %s ESCAPE '\\'",
        'endswith': "%s LIKE %s ESCAPE '\\'",
        'contains': "%s LIKE %s ESCAPE '\\'",
    }
    
    def get_connection(self, dsn):
        match = FILESYSTEM_DSN.match(dsn)
        
        if not match:
            raise InvalidDSN("'sqlite' adapter received an invalid DSN '%s'." % dsn)
        
        details = match.groupdict()
        
        import sqlite3
        return sqlite3.connect(details['path'])
    
    def raw(self, query, params=[], commit=True):
        cursor = self.connection.cursor()
        
        # SQLite returns a new cursor. Use that instead.
        try:
            result = cursor.execute(query, params)
            
            if commit:
                self.connection.commit()
        except:
            self.connection.rollback()
            raise
        
        return result
    
    def _get_column_names(self, table):
        if not table in self._tables:
            result = self.raw("SELECT * FROM %s" % table)
            self._tables[table] = sorted([column[0] for column in result.description])
        
        return self._tables[table]


class PostgresAdapter(BaseSQLAdapter):
    def get_connection(self, dsn):
        match = DAEMON_DSN.match(dsn)
        
        if not match:
            raise InvalidDSN("'postgres' adapter received an invalid DSN '%s'." % dsn)
        
        details = match.groupdict()
        
        import psycopg2
        return psycopg2.connect("dbname='%(database)s' user='%(user)s' password='%(password)s' host='%(host)s' port='%(port)s'" % details)
    
    def _get_column_names(self, table):
        query = "SELECT a.attname AS column \
        FROM pg_catalog.pg_attribute a \
        WHERE a.attnum > 0 \
        AND NOT a.attisdropped \
        AND a.attrelid = ( \
            SELECT c.oid \
            FROM pg_catalog.pg_class c \
            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
            WHERE c.relname ~ '^(%s)$' \
            AND pg_catalog.pg_table_is_visible(c.oid) \
        );" % table
        
        if not table in self._tables:
            result = self.raw(query.replace('\n', '').replace('\'', "'"), commit=False)
            
            if not result:
                raise QueryError("Table '%s' was not found or has no columns." % table)
            
            self._tables[table] = sorted([column[0] for column in result.fetchall()])
        
        return self._tables[table]


class MySQLAdapter(BaseSQLAdapter):
    def get_connection(self, dsn):
        match = DAEMON_DSN.match(dsn)
        
        if not match:
            raise InvalidDSN("'mysql' adapter received an invalid DSN '%s'." % dsn)
        
        details = match.groupdict()
        connection_details = {}
        
        for key, value in details.items():
            if key == 'database':
                connection_details['db'] = details['database']
            elif key == 'user':
                connection_details['user'] = details['user']
            elif key == 'host':
                connection_details['host'] = details['host']
            elif key == 'password':
                connection_details['passwd'] = details['password']
            elif key == 'port' and details['port']:
                connection_details['port'] = int(details['port'])
        
        import MySQLdb
        return MySQLdb.connect(**connection_details)
    
    def _get_column_names(self, table):
        query = "DESC %s;" % table
        
        if not table in self._tables:
            result = self.raw(query, commit=False)
            
            if not result:
                raise QueryError("Table '%s' was not found or has no columns." % table)
            
            self._tables[table] = sorted([column[0] for column in result.fetchall()])
        
        return self._tables[table]


class Bitty(object):
    ADAPTERS = {
        'sqlite': SQLiteAdapter,
        # 'json': JSONAdapter,
        'mysql': MySQLAdapter,
        'postgres': PostgresAdapter,
    }
    
    def __init__(self, dsn):
        """
        Valid DSNs::
        
            * sqlite:///Users/daniellindsley/test.db
            * postgres://daniel:my_p4ss@localhost:5432/test_db
            * mysql://daniel:my_p4ss@localhost:/test_db
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
    
    def close(self, commit=True):
        return self.adapter.close(commit=commit)
