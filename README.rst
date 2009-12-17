========
bitty.py
========

A tiny storage layer.


Why another storage layer?
==========================

I wanted one that was small (both in terms of a single file and in actual kloc),
tested and could handle multiple data stores. And because it was fun.

Example
=======

``bitty`` supports the usual CRUD methods. For example::

    from bitty import *
    
    bit = Bitty('sqlite:///home/code/my_database.db')
    
    bitty.add('people', name='Claris', says='Moof!', age=37)
    bitty.add('people', name='John Doe', says='No comment.', age=37)
    
    # Select all.
    for row in bitty.find('people'):
        print row['name']
    
    bit.close()


Supported Backends
==================

* SQLite
* Postgres
* MySQL


Schema
======

You're responsible for your own schema. bitty does the smallest amount of
introspection it can to get by.


When to use ``bitty``?
======================

``bitty`` is best used in environments like resource-based APIs and when 
integrating with other software (like a bigger web framework).

Tastes great when used with ``itty`` (http://github.com/toastdriven/itty).

Serious Python Programmers™ with Enterprise Requirements need not apply.


:author: Daniel Lindsley
:date: 2009-12-16