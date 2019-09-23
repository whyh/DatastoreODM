# DatastoreORM
### Simple ODM for Cloud Firestore in Datastore mode

Created to ease usage of the official client by structuring data with Django-like constructor

## How to use
```python
from orm import *

# Create scheme
class Book(Kind):
    class AuthorField(EmbeddedBaseField):
        id = IntegerField()
        name = StringField()

    title = StringField(index=True)
    language = StringField(valid=lambda value: True if value == "en" or value == "ja" else False, default="other")
    released = BooleanField(index=True, default=False)

    author = AuthorField(default=AuthorField)
    co_authors = ListField(AuthorField)

    n_borrowed = IntegerField(default=0)


# Create entity
new_book = Book("No Longer Human", "ja", released=True)
# If not specified, Id will be allocated and assigned during the instance creation
print(new_book.id)
new_book.author.name = "Osamu Dazai"

# Upsert entity (Create / Update)
new_book.save()

# Find record by id
book = Book.find(new_book.id)
print(book.title)

# Query database
released_books = Book.find_where(released=True, language="ja", _quantity=3)
for book in released_books:
    print(book.title)

# Delete record
new_book.delete()

# Find a record by id or create one with given id and values if it wasn't found
book = Book.find_or_create(new_book.id, **new_book.entity)
print(book.title)
```

## Supported fields
`IntegerField`  `FloatField`  `BooleanField`  `StringField`  `DatatimeField`  
`ListField`  `DictField`  
`EmbeddedField`  
Custom iterable fields can be created by inheriting `ComplexBaseField`, and non-iterable by inheriting `BaseField`  


## Field arguments
- `index` If a field should be indexed  
    Type `bool`. Default `False`  
- `default` Default value for a field  
    Type `Any` (`Callable` will be executed to retrieve a value). Default `None`  
- `valid` Function to validate a value  
    Type `Callable[[Any], bool]`. Default `lambda _: True`  

## Few cool features
#### Transaction wrapper
Comfortable way of performing atomic mutations
```python
@db.transaction(retry=True, retry_timeout=1)
def cleanup(*args, batch, **kwargs) -> Any:
    # perform mutations
    return # something
```
#### Pessimistic lock
Seamless pessimistic lock on reading(using the ORM). Definatly not best practice, but sometimes quite usefull
```python
class Book(Kind):
    _p_lock = True # To activate
    ...
```
#### Custom Kind
By default, a `Kind` is retrieved from class name of an instance, but you can specify your own
```python
class Book(Kind):
    _kind = "notbook"
    ...
```
#### Inheritance
`Kind` and `EmbeddedBaseField` children are safe to inherit from in a linear fashion
```python
class Book(Kind):
    title = StringField()


class LockedBook(Book):
    _kind = "book"
    _p_lock = True
```
