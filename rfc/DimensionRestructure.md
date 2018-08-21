Dimension Restructure RFC
-------------------------

Dimensions in 0.9 and before have several responsibilities.
They serve as a config entity for table schemas and representing metadata.  They are used as the hook for query
serialization.  They serve as a client for dimension value (domain) loading.  They act as a gateway to search
dimension values.

This refactor is oriented toward simplifying and structuring Dimensions.  The existing Dimension interface will
continue to provide the core access point for query and schema planning.

* Value resolution (filtered search and key lookup) will be rebased onto  the 'IndexedDomain' class.  

* Pure dimension metadata (state that doesn't drive any internal system behavior) will be held in an extensible
`DimensionDescriptor`

* Domains and Dimensions will have distinct schemas, `DomainSchema` schemas reflect all the columns in the logical 
domain, while `ApiDimensionSchema` can be a view on those values as well as a 'default columns'
used for response annotation.  Domain schemas can be shared between dimensions, and those dimensions could have distinct
views on the domain records.

* Domain endpoint /domain/DOMAINNAME/values should return domain values in the Domain Schema.  Dimension values 
/dimension/DIMENSIONNAME/values should return values in the ApiDimensionSchema columns.

* Cardinality will become an optional contract on dimensions (via Indexed domains)  

* Domains which are not browseable may return OptionalInt.empty() for cardinality and no values at dimension and 
domain values endpoints.

* The `Dimension` interface will no longer support explicit mutation of domain or index data.  Instead systems which 
wish to modify those should hold onto references to `MutableDomain`, etc.

New interfaces:

`DomainSchema`/`ApiDimensionSchema`:  Anything to do with dimension fields.

`DimensionDescriptor`: Name, description, category, etc.

`Domain`: Schema, cardinality, finderByKey

`IndexedDomain`: Domain, findByFilters, findAll

`MutableKeyValueStore`: not all KVS are mutable by default anymore



