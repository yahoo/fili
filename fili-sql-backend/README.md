# Using a SQL Backend
Fili has initial support for using a sql database as the backend instead of druid. Please
note that there are some restrictions since Fili is optimized for Druid.

## Setup
- Schema
    ```
    "PUBLIC" (i.e. SELECT * FROM `PUBLIC`.`TABLE_NAME`)
    ```
    Note: The default is public, so you only need to set it if yours is created a custom one like below.
        
    ```sql
    CREATE SCHEMA `THE_SCHEMA`;
    SET SCHEMA `THE_SCHEMA`;
    ```
- database url
    ```
    "jdbc:h2:mem:test"
    ```
- Driver
    ```
    "org.h2.Driver"
    ```
- username
- password

## Todo
- Implement TopN/nested queries/any other queries
- Add a flag or some way to signal queries should be sent to a sql backend instead of druid


## Limitations

### Supported queries
* timeseries
* groupBy

### Notable Restrictions

- As of now this has only been tested with H2 but it should work for any MYSQL database
- The time column must be a timestamp
- There's no way to detect if data is unavailable for a certain period of time
- There's no way to enable caching
