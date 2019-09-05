-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

--[[
This is where we define dimensions to be used in Fili. Dimensions provide a
means of attaching context to metrics. For example, with dimensions we don't
just know how many "pageViews" our website has had. We can also partition
"pageViews" into the number of men who've seen our pages, the number of women,
and the number of people whose gender is unknown.

Although we typically conceive of dimensions as being flat values (i.e. the
"male", "female" and "unknown" values for gender), in Fili each dimension is
in fact a table. Each dimension has a set of fields, which provide additional
information about the dimension. Each dimension value corresponds to a
particular tuple in the dimension table.

For example, the gender dimension may have the id, and name fields. So then
gender in Fili may look like:

        Gender
---------------------
|   ID  |   Name    |
|   0   |   male    |
|   1   |   female  |
|   2   |   unknown |
---------------------

Each tuple (0, Male), (1, Female) and (2, Unknown) is referred to as a
dimension row.
--]]
local dimensionUtils = require(LUTHIER_CONFIG_DIR .. 'utils.dimensionUtils')

-------------------------------------------------------------------------------
-- FieldSets
--[[
    Fields define a dimension's "metadata." For example, the country dimension
    may have the fields id, name, desc, and ISO. id is a unique identifier for
    the country (typically this is the primary key in your dimension database),
    name is a human readable name of the country, desc is a brief description
    of the country, and ISO is the country' ISO code.

    Fieldsets are lists of fields that may be attached to dimensions. Despite
    being Lua lists, field order is immaterial.

    A field is a table with at least one parameter:
        1. name - The name of the field
    Fields may also have the optional parameter:
        1. tags - A list of tags that may provide additional information about
            a field. For example, the "primaryKey" tag is used to mark a field
            as the dimension's primary key.

    To aid in configuration, we provide two utility functions for creating
    fields:
        1. pk - Takes a name and returns a primary key field. A primary key
        field is a table with two keys:
                a. name - The name of the field
                b. tags - A singleton list containing the value "primaryKey"
        2. field - A function that takes a variable number of field names and
        returns field for each name. Each field is a table with
        one key:
                a. name - The name passed in for that field
--]]
-------------------------------------------------------------------------------

local pk = dimensionUtils.pk
local field = dimensionUtils.field

-------------------------------------------------------------------------------
-- Dimensions
--[[
    Dimensions are defined in a table dimensions that maps dimension names to
    a dictionary of top-level information about that dimension:

    * apiName - The name to use when grouping on or filtering by this dimension.
        Defaults to the dimension's table key
    * longName - A longer human friendly name for the dimension
        Defaults to the dimension's table key
    * description - Brief documentation about the dimension
        Defaults to the dimension's table key
    * fields - A fieldset (see FieldSets above) describing the fields attached
        to the dimension
    * domain - An identifier for search provider and key value store
        Defaults to the dimension's table key
    * category - An arbitrary category to put the dimension in. This is not
        used directly by Fili, but rather exists as a marker for UI's should
        they desire to use it to organize dimensions.
    * type - A string that indicates the kind of dimension, used in build-time.
        Defaults to "KeyValueStoreDimension"
    * searchProvider - Refers to a template in searchProviderTemplates.lua
        includes the fully qualified Java class name of the SearchProvider
        and additional arguments to construct the SearchProvider.
        A SearchProvider is a service that searches for dimensions based
        on their dimension fields. For example, a SearchProvider can find all
        countries that have the string "States" in their name.
    * keyValueStore - A service that handles point look ups based
        on dimension ID.

    To aid in configuration, the dimensionUtils module provides two tables,
    searchProviders and keyValueStores that map a terse name to the class names
    of the built in SearchProviders and KeyValueStores.

    The built in search providers are:
        lucene - Backed by [Apache Lucene](https://lucene.apache.org/).
            Recommended for large (>10K values) dimensions.
        memory - Backed by an in-memory data structure.
            May be used for smallish (<10K values) dimensions.
        noop - Does not perform search. Useful for dimensions that only have
            an ID field.

    The built in key value stores are:
        redis - Backed by [Redis](https://redis.io/).
            Requires setup of a Redis cluster.
        memory - Backed by an in-memory map data structure.
            Recommended for smallish (<10K values) dimensions
]]
-------------------------------------------------------------------------------

return {
    testDimension = {
        longName = "a longName for testing",
        description = "a description for testing",
        fields = {
            pk("id"),
            field("desc")
        },
        defaultFields = {
            "desc"
        },
        category = "a category for testing",
        type = "KeyValueStoreDimension",
        isAggregatable = true,
        dimensionDomain = "testDomain",
        searchProvider = "memory",
        keyValueStore = "memory",
        skipLoading = true
    }
}
