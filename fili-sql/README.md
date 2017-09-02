Using a SQL Backend
====================
Fili has initial support for using a sql database as the backend instead of druid. Please
note that there are some restrictions since Fili is optimized for Druid.

Setup
-----
- Add a dependency on `fili-sql`
    ```xml
    <dependency>
        <groupId>com.yahoo.fili</groupId>
        <artifactId>fili-core</artifactId>
    </dependency>
    ```

- Set your application config
    - `bard__database_url = jdbc:h2:mem:test`
    - `bard__database_driver = org.h2.Driver`
    - `bard__database_username = username`
    - `bard__database_password = password`

- Make sure you build all of your SQL backed tables using `ConcreteSqlPhysicalTableDefinition`
    - Also note that your SQL backed tables may use any timezone but the easiest to work with will always be UTC.

- Override these from `AbstractBinderFactory`

    ```java
    @Override
    protected Class<? extends RequestWorkflowProvider> getWorkflow() {
        return SqlWorkflow.class;
    }
    
    @Override
    protected DimensionValueLoadTask buildDruidDimensionsLoader(
            DruidWebService webService,
            PhysicalTableDictionary physicalTableDictionary,
            DimensionDictionary dimensionDictionary
    ) {
        DruidDimensionValueLoader druidDimensionRowProvider = new DruidDimensionValueLoader(
                physicalTableDictionary,
                dimensionDictionary,
                Collections.emptyList(), // Put Druid Dimensions here if Applicable
                webService
        );
        SqlDimensionValueLoader sqlDimensionRowProvider = new SqlDimensionValueLoader(
                    physicalTableDictionary,
                    dimensionDictionary,
                    Arrays.asList("all", "the", "sql", "dimensions"), // Put Sql dimensions here
                    sqlBackedClient //build this
            );
        return new DimensionValueLoadTask(Arrays.asList(druidDimensionRowProvider, sqlDimensionRowProvider));
    }
    ```

You'll also want to make sure you load your SQL backed dimensions using `SqlDimensionValueLoader` in the `DimensionValueLoadTask`.

Supported queries
------------------
* timeseries
* groupBy
* topN (Requires: `bard__top_n_enabled=false` to be set)

### Notable Restrictions

- This has only been tested with H2 but it should work for most databases
- Your time column must be a timestamp
- There's no way to detect show availability of data
- There's no way to enable caching
- Character sets. Don't try to query unicode characters unless your database supports them.
- MySQL databases are **much slower** than druid at doing TopN queries.
 *Do **NOT** make TopN queries over large amounts of time with small buckets.* (For example, don't ask for the TopN from every hour over an entire year.) 