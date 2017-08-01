# Using a SQL Backend
Fili has initial support for using a sql database as the backend instead of druid. Please
note that there are some restrictions since Fili is optimized for Druid.

## Setup
- Set your database url
    - `bard__database_url = jdbc:h2:mem:test`
    
- Set your database driver
    - `bard__database_driver = org.h2.Driver`
    
- Set your database schema (default is `PUBLIC`)
    - `bard__database_schema =` 
        
        NOTE: this is probably fine unless you did `create schema "name"; set schema "name";`
        (i.e. `SELECT * FROM "SCHEM_NAME"."TABLE_NAME";`)
        
- Set your username/password
    - `bard__database_username = `
    - `bard__database_password = `
    
You'll also want to make sure you don't load any SQL backed dimensions using `DruidDimensionsLoader`.

## Todo
- Implement lookback/nested queries/any other queries

## Limitations
- Character sets. Basically don't try to query unicode characters unless your database supports them.
- MySQL databases are **much slower** than druid at doing TopN queries.
 *Do **NOT** make TopN queries over large amounts of time with small buckets.* (For example, don't ask for the TopN from every hour over an entire year.) 

### Supported queries
* timeseries
* groupBy
* topN (Requires: `bard__top_n_enabled=false` to be set)

### Notable Restrictions

- As of now this has only been tested with H2 but it should work for most databases
- You can only have one time column and it must be a timestamp
- There's no way to detect show availability of data
- There's no way to enable caching