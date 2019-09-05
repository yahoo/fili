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
|   id  |   Name    |
|   0   |   male    |
|   1   |   female  |
|   2   |   unknown |
---------------------

Each tuple (0, Male), (1, Female) and (2, Unknown) is referred to as a 
dimension row.
--]]
--local CONFIG_DIR = require('config').CONFIG_DIR
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
                returns an equal number of fields. Each field is a table with
                one key:
                a. name - The name passed in for that field
--]]
-------------------------------------------------------------------------------

local pk = dimensionUtils.pk
local field = dimensionUtils.field

local FIELDSETS = {
    default = { pk "id", field "desc" },
    country = { pk "id", field("desc", "county", "state") },
    page = { pk "id", field "desc" },
}

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
    * keyValueStore - The fully qualified Java class name of the KeyValueStore
        to use. A KeyValueStore is a service that handles point look ups based
        on dimension id.

    To aid in configuration, the dimensionUtils module provides two tables,
    searchProviders and keyValueStores that map a terse name to the class names
    of the built in SearchProviders and KeyValueStores.

    The built in search providers are:
        lucene - Backed by [Apache Lucene](https://lucene.apache.org/).
            Recommended for large (>10K values) dimensions.
        memory - Backed by an in-memory data structure.
            May be used for smallish (<10K values) dimensions.
        noop - Does not perform search. Useful for dimensions that only have
            an id field.

    The built in key value stores are:
        redis - Backed by [Redis](https://redis.io/).
            Requires setup of a Redis cluster.
        memory - Backed by an in-memory map data structure.
            Recommended for smallish (<10K values) dimensions
]]
-------------------------------------------------------------------------------

local M = {
    testDimension = {
        longName = "a longName for testing",
        description = "a description for testing",
        fields = {
            pk("id"),
            field("testField1", "testField2", "testField3")
        },
        category = "a category for testing",
        type = "KeyValueStoreDimension",
        isAggregatable = false,
        defaultFields = { "id", "testField1" },
        dimensionDomain = "testDomain",
        searchProvider = "lucene",
        keyValueStore = "memory"
    },
    comment = {
        longName = "wiki comment",
        description = "Comment for the edit to the wiki page",
        fields = FIELDSETS.default,
        category = "General",
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    countryIsoCode = {
        longName = "wiki countryIsoCode",
        description = "Iso Code of the country to which the wiki page belongs",
        fields = FIELDSETS.country,
        category = "General",
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    regionIsoCode = {
        longName = "wiki regionIsoCode",
        description = "Iso Code of the region to which the wiki page belongs",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    page = {
        longName = "wiki page",
        description = "Page is a document that is suitable for World Wide Web and web browsers",
        fields = FIELDSETS.page,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    user = {
        longName = "wiki user",
        description = "User is a person who generally use or own wiki services",
        fields = { pk "id", field("desc", "age", "sex") },
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    isUnpatrolled = {
        longName = "wiki isUnpatrolled",
        description = "Unpatrolled are class of pages that are not been patrolled",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    isNew = {
        longName = "wiki isNew",
        description = "New Page is the first page that is created in wiki",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    isRobot = {
        longName = "wiki isRobot",
        description = "Robot is an tool that carries out repetitive and mundane tasks",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    isAnonymous = {
        longName = "wiki isAnonymous",
        description = "Anonymous are individual or entity whose identity is unknown",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    isMinor = {
        longName = "wiki isMinor",
        description = "Minor is a person who is legally considered a minor",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    namespace = {
        longName = "wiki namespace",
        description = "Namespace is a set of wiki pages that begins with a reserved word",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    channel = {
        longName = "wiki channel",
        description = "Channel is a set of wiki pages on a certain channel",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    countryName = {
        longName = "wiki countryName",
        description = "Name of the Country to which the wiki page belongs",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    regionName = {
        longName = "wiki regionName",
        description = "Name of the Region to which the wiki page belongs",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    metroCode = {
        longName = "wiki metroCode",
        description = "Metro Code to which the wiki page belongs",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    cityName = {
        longName = "wiki cityName",
        description = "Name of the City to which the wiki page belongs",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    ["PT08.S2(NMHC)"] = {
        longName = "PT08.S2(NMHC)",
        description = "PT08.S2(NMHC)",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    ["PT08.S4(NO2)"] = {
        longName = "PT08.S4(NO2)",
        description = "PT08.S4(NO2)",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    ["PT08.S4(NO2)"] = {
        longName = "NO2(GT)",
        description = "NO2(GT)",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    ["C6H6(GT)"] = {
        longName = "C6H6(GT)",
        description = "C6H6(GT)",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    ["PT08.S1(CO)"] = {
        longName = "PT08.S1(CO)",
        description = "PT08.S1(CO)",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    ["NOx(GT)"] = {
        longName = "NOx(GT)",
        description = "NOx(GT)",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    RH = {
        longName = "RH",
        description = "RH",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    AH = {
        longName = "AH",
        description = "AH",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    ["NMHC(GT)"] = {
        longName = "NMHC(GT)",
        description = "NMHC(GT)",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    T = {
        longName = "T",
        description = "T",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    ["PT08.S3(NOx)"] = {
        longName = "PT08.S3(NOx)",
        description = "PT08.S3(NOx)",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    ["PT08.S5(O3)"] = {
        longName = "PT08.S5(O3)",
        description = "PT08.S5(O3)",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    },
    ["CO(GT)"] = {
        longName = "CO(GT)",
        description = "CO(GT)",
        fields = FIELDSETS.default,
        searchProvider = "memory",
        keyValueStore = "memory"
    }
}

--- set every dimension to be available at start, i.e. skip loading.
for _, dimension in pairs(M) do
    dimension.skipLoading = true
end

return M
