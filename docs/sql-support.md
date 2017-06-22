# Using a SQL Backend
Fili has initial support for using a sql database as the backend instead of druid. Please
note that there are some restrictions since Fili is optimized for Druid.

## Setup
- Set your database schema
    - `bard_database_schema = public` 
        
        NOTE: this is fine unless you did `create schema "name"; set schema "name";`
        (i.e. `SELECT * FROM "SCHEM_NAME"."TABLE_NAME";`)
    
- Set your database url
    - `bard__database_url = jdbc:h2:mem:dbName`
    
- Set your database driver (TODO make sure to include maven repo if needed)
    - `bard_database_driver = org.h2.Driver`
- Set your username/password
    - `bard__database_username = `
    - `bard__database_password = `

## Todo
- Actually create configuration settings above
- Implement sorting/paging
- Implement lookback/nested queries/any other queries
- Add a proper signal for queries to be sent to sql instead of druid

## Limitations
- Character sets. Basically don't try to query unicode characters unless your database supports them.

### Supported queries
* timeseries
* groupBy
* topN

### Notable Restrictions

- As of now this has only been tested with H2 but it should work for most databases
- You can only have one time column and it must be a timestamp
- There's no way to detect show availability of data
- There's no way to enable caching