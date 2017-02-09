Support Union Data Source Well
==============================
General Plan
------------
1. Physical Tables can be nested inside each other
2. A way to make queries built with a single Physical Table resolve to a Union Data Source if the Physical Table has children
- Make all DataSources into Unions?
  - PT.getChildNames?
- Make DataSource creation smarter? (ie. 1 total table: Table, many: Union)
  - 
- Convert from Table(s) to Union?
- Want to be able to resolve into multiple tables at some point
- 
PT, ser (convert) to name
UnionPhysicalTable
------------------
1. IS A Physical Table
2. Has collection of "backing" tables
3. Constructor should:
    - Validate child table grains satisfy parent table's grain
    - Validate LogicalToPhysicalColumnNames maps agree on mappings for common logical names
    - Aggregate name mappings from children into itself
4. addColumn throws UnsupportedOperationException
5. removeColumn throws UnsupportedOperationException
6. resetColumns throws UnsupportedOperationException
7. commit throws UnsupportedOperationException
8. getColumns merges sets of columns from child tables
9. getAvailableIntervals must do column-wise union of available intervals
10. getIntervalsByColumnName must union sets
Make DruidQueryBuilder able to build UnionDataSource if it gets a UnionPhysicalTable
Config
- Extend PhysicalTableDefinition?
Query
- Make builder smarter
Segment Metadata
Data Structure
- Extend PhysicalTable
WritableAvailabilityProvider
- Not on PhysicalTableInterface
----------------------
- Refactor to make PhysicalTable extensible
  - Clean up Schema / Table / PhysicalTable model
  - Move to DataSourceMetadata-based Availability
    - Clean up cruft from old / deprecated SegmentMetadataLoader mechanism
    - Clean up metadata package
- Extend PhysicalTable to make 
Schema
- Extract HasGranularity interface
- Make interface-only: Schema extends Set<Column>, HasGranularity
- Rename getColumns -> getColumnsByType, as default method
- Remove getColumn(byName)
- Remove getColumn(byName, byType)
Table
- Extract HasName interface
- Make interface-only: Table extends Schema, HasName
- Make getDimensions default method
Physical Table
- Make implement Table
- Remove getTablePeriod (unused)
- Remove getWorkingIntervals (only used in tests)
- Would be nice to remove hasLogicalMapping
- Make getTableAlignment private
- Extract ColumnAvailabilityService interface (into top-level metadata package)
  - getIntervalsByColumnName, though it might be nice to be able to _not_ keep it around (used in 1 place)
  - getIntervals (though it's only used in the SlicesServlet...)
  - Implement with DataSourceColumnAvailabilityService (into top-level metadata package)
    - Make it UnionPhysicalTable-aware
      - 
    - May be able to use some static methods from DataSourceMetadata
      - May want to use JMH to performance-test which of them is faster
        - Common-case is many small, contiguous segments
        - Next common is to have small number of holes
  - Switch PartialDataHandler and SlicesServlet require a ColumnAvailabilityService and use it for availability instead of PhysicalTable
DataSourceMetadataLoader
- Make run PhysicalTableType-aware (don't run for UnionPhysicalTables)
SegmentMetadataLoader
- Remove it (deprecated in favor of non-PhysicalTable-attached DataSourceMetadataService)
- Remove references to it in AbstractBinderFactory
- Remove SegmentMetadataLoaderHealthcheck
- Remove setupPartialData method from AbstractBinderFactory
SegmentMetadata
- Remove (only used for SegmentMetadataLoader flows)
TimeBoundaryResponse
- Remove (unused)
SegmentInfo
- Remove shardSpec (unused)
RequestedIntervalsFunction
- Move to druid.model.query (it's more relevant to that package)
