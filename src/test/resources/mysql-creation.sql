create SCHEMA ${database};
CREATE USER '${user}'@'localhost' IDENTIFIED BY '${password}';
CREATE USER '${user}'@'%' IDENTIFIED BY '${password}';
GRANT ALL ON *.* TO '${user}'@'localhost' IDENTIFIED BY '${password}' WITH GRANT OPTION;
GRANT ALL ON *.* TO '${user}'@'%' IDENTIFIED BY '${password}' WITH GRANT OPTION;

USE ${database};

-- create a table to allow tests to query
CREATE TABLE testtable (
    dbname varchar(50)
);

INSERT INTO testtable (dbname)
VALUES ('${database}');
