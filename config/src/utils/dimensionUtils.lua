-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

--- A module that provides util functions for dimension config.
--
-- Furthermore, it provides two tables:
--  searchProviders - A mapping from the name of each search provider to its
--      full Java class name
--  keyValueStores - A mapping from the name of each keyValueStore to its
--      full Java class name
-- @module dimensionUtils

local M = {}

local misc = require 'utils/misc'

-------------------------------------------------------------------------------
-- Fields
-------------------------------------------------------------------------------

--- Generates a field with the specified name and the "primaryKey" tag.
--
-- @param name the field's name
-- @return a field with a "primaryKey" tag
function M.pk(name)
    return { name = name, tags = { "primaryKey" } }
end

--- Generates a set of fields without tags.
--
-- @param ...  The names of the fields to generate
-- @return a variable number of fields without tags, where each field's name is
-- one of the passed in names
function M.field(...)
    local args = {...}
    local fields = {}
    for _, name in pairs(args) do
        table.insert(fields, {name=name})
    end
    return table.unpack(fields)
end

-------------------------------------------------------------------------------
-- Build Config
-------------------------------------------------------------------------------

--- Parse a group of config dimensions and add them into a table.
--
-- @param dimensions  A group of dimensions
-- @return dimension config, ready for parsing into json
function M.build_dimensions_config(dimensions)
    local configuration = {}
    for name, dimension in pairs(dimensions) do
        local dim_copy = misc.shallow_copy(dimension)
        dim_copy.apiName = dim_copy.apiName or name
        dim_copy.longName = dim_copy.longName or name
        dim_copy.description = dim_copy.description or name
        table.insert(configuration, dim_copy)
    end
    return configuration
end

M.searchProviders = {
    lucene =
        "com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProvider",
    noop = "com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProvider",
    memory = "com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProvider"
}

M.keyValueStores = {
    memory = "com.yahoo.bard.webservice.data.dimension.MapStore",
    redis = "com.yahoo.bard.webservice.data.dimension.RedisStore"
}

return M
