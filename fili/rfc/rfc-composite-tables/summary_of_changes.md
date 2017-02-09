Make Physical Tables Concrete to prepare for Non-concrete
=========================================================

Top level goals:

* Enrich Physical Table Availability concepts to allow delegated availability
* Separate out PhysicalTable schema concerns from availability
* Isolate schema mutability to isolate mutating state on Tables

Implementations:

* `Availability` interface replaces passing Map<Column, Set<Interval>>
   * Explicit interfaces around immutable and mutable state
   * Deprecated most partial-change interfaces

* Schema mutating on PhysicalTable deprecated with prejudice
   * Columns changed to allow/prefer construction before Schema, create Schemas immutably 
   * Schema and Table transformed to interfaces
      * Table goes from being a schema to having a schema
      * Explicit Schema instances  
         * LogicalTableSchema (base non grain schema)
         * ResultSetSchema (Granular schema with an withAddColumn() helper method)
         * PhysicalTableSchema (absorbs column name mapping responsibilities from physical table)

* `DruidQueryBuilder` shifted to only use ConcretePhysicalTables until CompositePhysicalTable is available
* `ZonedSchema` used by ResultSet.schema deprecated because it was identified as being more trouble than it was worth.

* Assorted import changes will be reverted when classes come out of RFC package
