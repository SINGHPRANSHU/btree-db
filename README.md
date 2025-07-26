# btree-db

Simple database with select, create and delete functionality. It uses btree for indexing. Currently only primary indexes are supported which requires id column to be present that should be added while creating new table. Add spaces after every token for parser to work correctly. Currently queries are case sensitive.



Steps to run

1. to start db server run
```
go run .
```
2. use cli to access db.

```
cd src/connector
go run connector.go
```

List of Statements that can be executed - 

## Create 

integer wil always be of size 8 and char can be of any size.


```
CREATE TABLE tests id Integer 8 test2 Char 10 test3 Integer 8
```

## Insert

while inserting pass all col as currently no default are supported

```
INSERT into tests (id, test2, test3) values (1, abc, 4)
```

## SELECT

select will work with where clause with "id" col only. currenlty select cannnot fetch all record.

```
SELECT * FROM tests WHERE id = 1
```

## DELETE 

delete will work with where clause with "id" col only


```
DELETE FROM tests WHERE id = 1
```