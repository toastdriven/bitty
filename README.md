bitty.py
========

A tiny storage layer.


Why another storage layer?
--------------------------

I wanted one that was small (both in terms of a single file and in actual kloc),
tested and could handle multiple data stores. And because it was fun.

Example
-------

`bitty` supports the usual CRUD methods. For example::

    from bitty import *
    
    bit = Bitty('sqlite:///home/code/my_database.db')
    
    bit.add('people', name='Claris', says='Moof!', age=37)
    bit.add('people', name='John Doe', says='No comment.', age=37)
    
    # Select all.
    for row in bit.find('people'):
        print row['name']
    
    bit.close()

See `examples/` and `tests.py` for more usages.


Supported Backends
------------------

* SQLite
* Postgres
* MySQL


Schema
------

You're responsible for your own schema. bitty does the smallest amount of
introspection it can to get by.


When to use bitty?
------------------

`bitty` is best used in environments like resource-based APIs and when 
integrating with other software (like a bigger web framework).

Tastes great when used with [itty][1].

Serious Python Programmers™ with Enterprise Requirements need not apply.

[1]: http://github.com/toastdriven/itty

*author: Daniel Lindsley*

*date: 2010-01-20*
