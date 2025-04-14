--create schema gold
CREATE SCHEMA gold;

--create view calender
CREATE VIEW gold.calender
AS
SELECT * FROM
OPENROWSET(
    BULK 'abfss://silver@awdatalakeuzer.dfs.core.windows.net/AdventureWorks_Calendar/',
    FORMAT = 'PARQUET'
) AS q1

--create view customer
CREATE VIEW gold.customer
AS
SELECT * FROM
OPENROWSET(
    BULK 'abfss://silver@awdatalakeuzer.dfs.core.windows.net/AdventureWorks_Customer/',
    FORMAT = 'PARQUET'
) AS q2

--create view product
CREATE VIEW gold.product
AS
SELECT * FROM
OPENROWSET(
    BULK 'abfss://silver@awdatalakeuzer.dfs.core.windows.net/AdventureWorks_Product/',
    FORMAT = 'PARQUET'
) AS q3

--create view returns
CREATE VIEW gold.returns
AS
SELECT * FROM
OPENROWSET(
    BULK 'abfss://silver@awdatalakeuzer.dfs.core.windows.net/AdventureWorks_Returns/',
    FORMAT = 'PARQUET'
) AS q4

--create view sales
CREATE VIEW gold.sales
AS
SELECT * FROM
OPENROWSET(
    BULK 'abfss://silver@awdatalakeuzer.dfs.core.windows.net/AdventureWorks_Sales/',
    FORMAT = 'PARQUET'
) AS q5

--create view subcategories
CREATE VIEW gold.subcategories
AS
SELECT * FROM
OPENROWSET(
    BULK 'abfss://silver@awdatalakeuzer.dfs.core.windows.net/AdventureWorks_SubCategories/',
    FORMAT = 'PARQUET'
) AS q6

--create view territories
CREATE VIEW gold.territories
AS
SELECT * FROM
OPENROWSET(
    BULK 'abfss://silver@awdatalakeuzer.dfs.core.windows.net/AdventureWorks_Territories/',
    FORMAT = 'PARQUET'
) AS q7

