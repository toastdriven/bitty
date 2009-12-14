from bitty import Bitty
import datetime

# Connect.
bit = Bitty('sqlite://test.db')

# Insert.
bit.add('people', id=1, name='Daniel', age=27, created=datetime.datetime.now())
bit.add('people', id=2, name='Moriah', age=27, created=datetime.datetime.now())
bit.add('people', id=3, name='Sean', age=3, created=datetime.datetime.now())
bit.add('people', id=4, name='Chester', age=1, created=datetime.datetime.now())

# Update?
bit.update('people', id=1, name='Toast Driven')

# Get.
toast = bit.get('people', id=1)

# Select.
if len(bit.find('people', id=1))):
    for result in bit.find('people', id=1):
        print result['name']

for result in bit.find('people', name=['Moriah', 'Sean']):
    print result['name']

for result in bit.find('people', age__lte=20):
    print result['name']

for result in bit.find('people', name=['Moriah', 'Sean'], age__lte=20)
    print result['name']

# Delete?
bit.delete('people', id=1)
