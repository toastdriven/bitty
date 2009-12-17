from bitty import *
import MySQLdb
import os
import psycopg2
import sqlite3
import unittest


class MockBaseSQLAdapter(BaseSQLAdapter):
    def get_connection(self, dsn):
        return None
    
    def _get_column_names(self, table):
        if table == 'test':
            return ['id', 'text']
        
        return ['age', 'id', 'name']


class BaseSQLAdapterTestCase(unittest.TestCase):
    def setUp(self):
        super(BaseSQLAdapterTestCase, self).setUp()
        self.base = MockBaseSQLAdapter('foo:///bar')
    
    def test_build_insert_query(self):
        self.assertEqual(self.base._build_insert_query('people', name='Daniel'), ('INSERT INTO people (name) VALUES (%s)', ['Daniel']))
        self.assertEqual(self.base._build_insert_query('people', id=1, name='Daniel'), ('INSERT INTO people (id, name) VALUES (%s, %s)', [1, 'Daniel']))
        self.assertEqual(self.base._build_insert_query('people', id=1, name='Daniel', age=27), ('INSERT INTO people (age, id, name) VALUES (%s, %s, %s)', [27, 1, 'Daniel']))
        self.assertEqual(self.base._build_insert_query('people', name='Daniel'), ('INSERT INTO people (name) VALUES (%s)', ['Daniel']))
        self.assertEqual(self.base._build_insert_query('test', text='foo'), ('INSERT INTO test (text) VALUES (%s)', ['foo']))
    
    def test_build_update_query(self):
        self.assertEqual(self.base._build_update_query('people', 1, name='Daniel'), ('UPDATE people SET name = %s WHERE id = %s', ['Daniel', 1]))
        self.assertEqual(self.base._build_update_query('people', 1, age=27), ('UPDATE people SET age = %s WHERE id = %s', [27, 1]))
        self.assertEqual(self.base._build_update_query('people', 2, name='Daniel', age=27), ('UPDATE people SET age = %s, name = %s WHERE id = %s', [27, 'Daniel', 2]))
        self.assertEqual(self.base._build_update_query('people', 10, name='Daniel'), ('UPDATE people SET name = %s WHERE id = %s', ['Daniel', 10]))
        self.assertEqual(self.base._build_update_query('test', 10, name='Daniel', age=27), ('UPDATE test SET age = %s, name = %s WHERE id = %s', [27, 'Daniel', 10]))
    
    def test_build_delete_query(self):
        self.assertEqual(self.base._build_delete_query('people', 1), ('DELETE FROM people WHERE id = %s', [1]))
        self.assertEqual(self.base._build_delete_query('people', 2), ('DELETE FROM people WHERE id = %s', [2]))
        self.assertEqual(self.base._build_delete_query('people', 10), ('DELETE FROM people WHERE id = %s', [10]))
        self.assertEqual(self.base._build_delete_query('test', '100'), ('DELETE FROM test WHERE id = %s', ['100']))
    
    def test_build_select_query(self):
        self.assertEqual(self.base._build_select_query('people'), ('SELECT age, id, name FROM people', []))
        self.assertEqual(self.base._build_select_query('people', id=1), ('SELECT age, id, name FROM people WHERE id = %s', [1]))
        self.assertEqual(self.base._build_select_query('people', name='Daniel'), ('SELECT age, id, name FROM people WHERE name = %s', ['Daniel']))
        self.assertEqual(self.base._build_select_query('people', id=1, name='Daniel'), ('SELECT age, id, name FROM people WHERE id = %s AND name = %s', [1, 'Daniel']))
        self.assertEqual(self.base._build_select_query('test', text='Daniel'), ('SELECT id, text FROM test WHERE text = %s', ['Daniel']))
    
    def test_get_column_names(self):
        self.assertEqual(self.base._get_column_names('people'), ['age', 'id', 'name'])
        self.assertEqual(self.base._get_column_names('test'), ['id', 'text'])
    
    def test_build_where_clause(self):
        self.assertEqual(self.base._build_where_clause(id=1), ('WHERE id = %s', [1]))
        self.assertEqual(self.base._build_where_clause(id=1, name='Daniel'), ('WHERE id = %s AND name = %s', [1, 'Daniel']))
        self.assertEqual(self.base._build_where_clause(name__startswith='Daniel'), ('WHERE name LIKE %s', ['Daniel%']))
        self.assertEqual(self.base._build_where_clause(name__endswith='Daniel'), ("WHERE name LIKE %s", ['%Daniel']))
        self.assertEqual(self.base._build_where_clause(name__contains='Daniel'), ("WHERE name LIKE %s", ['%Daniel%']))
        self.assertEqual(self.base._build_where_clause(id__lt=10), ('WHERE id < %s', [10]))
        self.assertEqual(self.base._build_where_clause(id__lte=10), ('WHERE id <= %s', [10]))
        self.assertEqual(self.base._build_where_clause(id__gt=10), ('WHERE id > %s', [10]))
        self.assertEqual(self.base._build_where_clause(id__gte=10), ('WHERE id >= %s', [10]))
        self.assertEqual(self.base._build_where_clause(id__in=[1, 2, 10]), ('WHERE id IN (%s, %s, %s)', [1, 2, 10]))
        # The motherload.
        self.assertEqual(self.base._build_where_clause(id=1, name='Daniel', lastname__startswith='Daniel', firstname__endswith='Daniel', address__contains='Daniel', age__lt=30, zip__lte=99999, favorite_count__gt=15, comments__gte=34, status__in=['active', 'banned']), ('WHERE address LIKE %s AND age < %s AND comments >= %s AND favorite_count > %s AND firstname LIKE %s AND id = %s AND lastname LIKE %s AND name = %s AND status IN (%s, %s) AND zip <= %s', ['%Daniel%', 30, 34, 15, '%Daniel', 1, 'Daniel%', 'Daniel', 'active', 'banned', 99999]))


class SQLiteTestCase(unittest.TestCase):
    def setUp(self):
        super(SQLiteTestCase, self).setUp()
        self.db_name = '/tmp/bitty_test.db'
        
        if os.path.exists(self.db_name):
            os.remove(self.db_name)
        
        self.base = Bitty("sqlite://%s" % self.db_name)
        self.base.raw("""CREATE TABLE people (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(255), age INTEGER NULL);""")
        self.base.raw("""CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, text VARCHAR(255));""")
        
        self.base.raw("""INSERT INTO people (id, name, age) VALUES (1, 'Daniel', 27);""")
        self.base.raw("""INSERT INTO people (id, name, age) VALUES (2, 'Foo', 7);""")
        self.base.raw("""INSERT INTO people (id, name, age) VALUES (3, 'Moof', 35);""")
        self.base.raw("""INSERT INTO test (id, text) VALUES (1, 'moof');""")
    
    def test_get_adapter(self):
        self.assertRaises(InvalidDSN, self.base.get_adapter, 'foo')
        self.assertRaises(InvalidDSN, self.base.get_adapter, 'foo://bar')
        self.assertRaises(sqlite3.OperationalError, self.base.get_adapter, 'sqlite:///')
        self.assert_(isinstance(self.base.get_adapter("sqlite://%s" % self.db_name), SQLiteAdapter))
    
    def test_add(self):
        self.assertEqual(self.base.add('people', name='Daniel'), True)
        self.assertEqual(self.base.add('people', name='Daniel', age=27), True)
        self.assertRaises(sqlite3.IntegrityError, self.base.add, 'people', id=1, name='Daniel')
        self.assertEqual(self.base.add('people', name='Daniel'), True)
        self.assertEqual(self.base.add('test', text='foo'), True)
    
    def test_update(self):
        self.assertEqual(self.base.update('people', 1, name='Daniel'), True)
        self.assertEqual(self.base.update('people', 1, age=27), True)
        self.assertEqual(self.base.update('people', 2, name='Daniel', age=27), True)
        self.assertEqual(self.base.update('people', 10, name='Daniel'), False)
        self.assertEqual(self.base.update('test', 1, text='bar'), True)
    
    def test_delete(self):
        self.assertEqual(self.base.delete('people', 1), True)
        self.assertEqual(self.base.delete('people', 2), True)
        self.assertEqual(self.base.delete('people', 10), False)
        # Wrong kind of pk.
        self.assertEqual(self.base.delete('test', '100'), False)
    
    def test_find(self):
        self.assertEqual(self.base.find('people'), [{'age': 27, 'id': 1, 'name': u'Daniel'}, {'age': 7, 'id': 2, 'name': u'Foo'}, {'age': 35, 'id': 3, 'name': u'Moof'}])
        self.assertEqual(self.base.find('people', id=1), [{'age': 27, 'id': 1, 'name': u'Daniel'}])
        self.assertEqual(self.base.find('people', name='Daniel'), [{'age': 27, 'id': 1, 'name': u'Daniel'}])
        self.assertEqual(self.base.find('people', id=1, name='Daniel'), [{'age': 27, 'id': 1, 'name': u'Daniel'}])
        self.assertEqual(self.base.find('test', text='Daniel'), [])
        
        # Test advanced lookups.
        self.assertEqual(self.base.find('people', id__gte=1), [{'age': 27, 'id': 1, 'name': u'Daniel'}, {'age': 7, 'id': 2, 'name': u'Foo'}, {'age': 35, 'id': 3, 'name': u'Moof'}])
        self.assertEqual(self.base.find('people', id__gte=1, name__startswith='Dan'), [{'age': 27, 'id': 1, 'name': u'Daniel'}])
        self.assertEqual(self.base.find('people', name__contains='a'), [{'age': 27, 'id': 1, 'name': u'Daniel'}])
    
    def test_get(self):
        self.assertEqual(self.base.get('people', name='Daniel'), {'age': 27, 'id': 1, 'name': u'Daniel'})
        self.assertEqual(self.base.get('people', id=1, name='Daniel'), {'age': 27, 'id': 1, 'name': u'Daniel'})
        self.assertEqual(self.base.get('people', id=1, name='Daniel', age=27), {'age': 27, 'id': 1, 'name': u'Daniel'})
    
    def test_raw(self):
        self.assertEqual(self.base.raw("DELETE FROM people;").rowcount, 0)
        self.assertEqual(self.base.raw("INSERT INTO people (id, name, age) VALUES (1, 'Daniel', 27);").rowcount, 1)
        self.assertEqual(self.base.raw("UPDATE people SET name = 'Toast Driven' WHERE id = 1;").rowcount, 1)
        self.assertEqual(self.base.raw("DELETE FROM people WHERE id = 1;").rowcount, 1)
    
    def test_regression_commit(self):
        self.assertEqual(self.base.add('people', name='Toasty'), True)
        
        alternate = Bitty("sqlite://%s" % self.db_name)
        self.assertEqual(alternate.find('people', name='Toasty'), [{'age': None, 'id': 4, 'name': u'Toasty'}])


class PostgresTestCase(unittest.TestCase):
    def setUp(self):
        super(PostgresTestCase, self).setUp()
        self.base = Bitty("postgres://postgres:@localhost/bitty_test")
        
        try:
            self.base.raw("""DROP TABLE people;""")
        except:
            self.base.adapter.connection.rollback()
        
        try:
            self.base.raw("""DROP TABLE test;""")
        except:
            self.base.adapter.connection.rollback()
        
        self.base.raw("""CREATE TABLE people (id SERIAL UNIQUE, name VARCHAR(255), age INTEGER NULL);""")
        self.base.raw("""CREATE TABLE test (id SERIAL UNIQUE, text VARCHAR(255));""")
        
        self.base.raw("""INSERT INTO people (name, age) VALUES ('Daniel', 27);""")
        self.base.raw("""INSERT INTO people (name, age) VALUES ('Foo', 7);""")
        self.base.raw("""INSERT INTO people (name, age) VALUES ('Moof', 35);""")
        self.base.raw("""INSERT INTO test (text) VALUES ('moof');""")
    
    def tearDown(self):
        self.base.close()
        super(PostgresTestCase, self).tearDown()
    
    def test_get_adapter(self):
        self.assertRaises(InvalidDSN, self.base.get_adapter, 'foo')
        self.assertRaises(InvalidDSN, self.base.get_adapter, 'foo://bar')
        self.assertRaises(InvalidDSN, self.base.get_adapter, 'postgres://localhost/test')
        self.assert_(isinstance(self.base.get_adapter("postgres://postgres:@localhost/bitty_test"), PostgresAdapter))
    
    def test_add(self):
        self.assertEqual(self.base.add('people', name='Daniel'), True)
        self.assertEqual(self.base.add('people', name='Daniel', age=27), True)
        self.assertRaises(psycopg2.IntegrityError, self.base.add, 'people', id=1, name='Daniel')
        self.assertEqual(self.base.add('people', name='Daniel'), True)
        self.assertEqual(self.base.add('test', text='foo'), True)
    
    def test_update(self):
        self.assertEqual(self.base.update('people', 1, name='Daniel'), True)
        self.assertEqual(self.base.update('people', 1, age=27), True)
        self.assertEqual(self.base.update('people', 2, name='Daniel', age=27), True)
        self.assertEqual(self.base.update('people', 10, name='Daniel'), False)
        self.assertEqual(self.base.update('test', 1, text='bar'), True)
    
    def test_delete(self):
        self.assertEqual(self.base.delete('people', 1), True)
        self.assertEqual(self.base.delete('people', 2), True)
        self.assertEqual(self.base.delete('people', 10), False)
        # Wrong kind of pk.
        self.assertEqual(self.base.delete('test', '100'), False)
    
    def test_find(self):
        self.assertEqual(self.base.find('people'), [{'age': 27, 'id': 1, 'name': u'Daniel'}, {'age': 7, 'id': 2, 'name': u'Foo'}, {'age': 35, 'id': 3, 'name': u'Moof'}])
        self.assertEqual(self.base.find('people', id=1), [{'age': 27, 'id': 1, 'name': u'Daniel'}])
        self.assertEqual(self.base.find('people', name='Daniel'), [{'age': 27, 'id': 1, 'name': u'Daniel'}])
        self.assertEqual(self.base.find('people', id=1, name='Daniel'), [{'age': 27, 'id': 1, 'name': u'Daniel'}])
        self.assertEqual(self.base.find('test', text='Daniel'), [])
        
        # Test advanced lookups.
        self.assertEqual(self.base.find('people', id__gte=1), [{'age': 27, 'id': 1, 'name': u'Daniel'}, {'age': 7, 'id': 2, 'name': u'Foo'}, {'age': 35, 'id': 3, 'name': u'Moof'}])
        self.assertEqual(self.base.find('people', id__gte=1, name__startswith='Dan'), [{'age': 27, 'id': 1, 'name': u'Daniel'}])
        self.assertEqual(self.base.find('people', name__contains='a'), [{'age': 27, 'id': 1, 'name': u'Daniel'}])
    
    def test_get(self):
        self.assertEqual(self.base.get('people', name='Daniel'), {'age': 27, 'id': 1, 'name': u'Daniel'})
        self.assertEqual(self.base.get('people', id=1, name='Daniel'), {'age': 27, 'id': 1, 'name': u'Daniel'})
        self.assertEqual(self.base.get('people', id=1, name='Daniel', age=27), {'age': 27, 'id': 1, 'name': u'Daniel'})
    
    def test_raw(self):
        self.assertEqual(self.base.raw("DELETE FROM people;").rowcount, 3)
        self.assertEqual(self.base.raw("INSERT INTO people (id, name, age) VALUES (1, 'Daniel', 27);").rowcount, 1)
        self.assertEqual(self.base.raw("UPDATE people SET name = 'Toast Driven' WHERE id = 1;").rowcount, 1)
        self.assertEqual(self.base.raw("DELETE FROM people WHERE id = 1;").rowcount, 1)


class MySQLTestCase(unittest.TestCase):
    def setUp(self):
        super(MySQLTestCase, self).setUp()
        self.base = Bitty("mysql://root:@localhost/bitty_test")
        
        try:
            self.base.raw("""DROP TABLE people;""")
        except:
            self.base.adapter.connection.rollback()
        
        try:
            self.base.raw("""DROP TABLE test;""")
        except:
            self.base.adapter.connection.rollback()
        
        self.base.raw("""CREATE TABLE people (id INTEGER PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255), age INTEGER NULL);""")
        self.base.raw("""CREATE TABLE test (id INTEGER PRIMARY KEY AUTO_INCREMENT, text VARCHAR(255));""")
        
        self.base.raw("""INSERT INTO people (name, age) VALUES ('Daniel', 27);""")
        self.base.raw("""INSERT INTO people (name, age) VALUES ('Foo', 7);""")
        self.base.raw("""INSERT INTO people (name, age) VALUES ('Moof', 35);""")
        self.base.raw("""INSERT INTO test (text) VALUES ('moof');""")
    
    def tearDown(self):
        self.base.close()
        super(MySQLTestCase, self).tearDown()
    
    def test_get_adapter(self):
        self.assertRaises(InvalidDSN, self.base.get_adapter, 'foo')
        self.assertRaises(InvalidDSN, self.base.get_adapter, 'foo://bar')
        self.assertRaises(InvalidDSN, self.base.get_adapter, 'mysql://localhost/test')
        self.assert_(isinstance(self.base.get_adapter("mysql://root:@localhost/bitty_test"), MySQLAdapter))
    
    def test_add(self):
        self.assertEqual(self.base.add('people', name='Daniel'), True)
        self.assertEqual(self.base.add('people', name='Daniel', age=27), True)
        self.assertRaises(MySQLdb.IntegrityError, self.base.add, 'people', id=1, name='Daniel')
        self.assertEqual(self.base.add('people', name='Daniel'), True)
        self.assertEqual(self.base.add('test', text='foo'), True)
    
    def test_update(self):
        self.assertEqual(self.base.update('people', 1, name='Danielr'), True)
        self.assertEqual(self.base.update('people', 1, age=26), True)
        self.assertEqual(self.base.update('people', 2, name='Daniel', age=27), True)
        self.assertEqual(self.base.update('people', 10, name='Daniel'), False)
        self.assertEqual(self.base.update('test', 1, text='bar'), True)
    
    def test_delete(self):
        self.assertEqual(self.base.delete('people', 1), True)
        self.assertEqual(self.base.delete('people', 2), True)
        self.assertEqual(self.base.delete('people', 10), False)
        # Wrong kind of pk.
        self.assertEqual(self.base.delete('test', '100'), False)
    
    def test_find(self):
        self.assertEqual(self.base.find('people'), [{'age': 27, 'id': 1, 'name': u'Daniel'}, {'age': 7, 'id': 2, 'name': u'Foo'}, {'age': 35, 'id': 3, 'name': u'Moof'}])
        self.assertEqual(self.base.find('people', id=1), [{'age': 27, 'id': 1, 'name': u'Daniel'}])
        self.assertEqual(self.base.find('people', name='Daniel'), [{'age': 27, 'id': 1, 'name': u'Daniel'}])
        self.assertEqual(self.base.find('people', id=1, name='Daniel'), [{'age': 27, 'id': 1, 'name': u'Daniel'}])
        self.assertEqual(self.base.find('test', text='Daniel'), [])
        
        # Test advanced lookups.
        self.assertEqual(self.base.find('people', id__gte=1), [{'age': 27, 'id': 1, 'name': u'Daniel'}, {'age': 7, 'id': 2, 'name': u'Foo'}, {'age': 35, 'id': 3, 'name': u'Moof'}])
        self.assertEqual(self.base.find('people', id__gte=1, name__startswith='Dan'), [{'age': 27, 'id': 1, 'name': u'Daniel'}])
        self.assertEqual(self.base.find('people', name__contains='a'), [{'age': 27, 'id': 1, 'name': u'Daniel'}])
    
    def test_get(self):
        self.assertEqual(self.base.get('people', name='Daniel'), {'age': 27, 'id': 1, 'name': u'Daniel'})
        self.assertEqual(self.base.get('people', id=1, name='Daniel'), {'age': 27, 'id': 1, 'name': u'Daniel'})
        self.assertEqual(self.base.get('people', id=1, name='Daniel', age=27), {'age': 27, 'id': 1, 'name': u'Daniel'})
    
    def test_raw(self):
        self.assertEqual(self.base.raw("DELETE FROM people;").rowcount, 3)
        self.assertEqual(self.base.raw("INSERT INTO people (id, name, age) VALUES (1, 'Daniel', 27);").rowcount, 1)
        self.assertEqual(self.base.raw("UPDATE people SET name = 'Toast Driven' WHERE id = 1;").rowcount, 1)
        self.assertEqual(self.base.raw("DELETE FROM people WHERE id = 1;").rowcount, 1)


if __name__ == '__main__':
    unittest.main()
