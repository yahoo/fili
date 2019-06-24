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

local default_type = "KeyValueStoreDimension"
local default_category = "UNKNOWN_CATEGORY"

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
        table.insert(fields, { name = name, tags = {} })
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
        dim_copy.longName = dim_copy.longName or name
        dim_copy.description = dim_copy.description or name
        dim_copy.type = dim_copy.type or default_type
        dim_copy.category = dim_copy.category or default_category
        dim_copy.defaultFields = dim_copy.defaultFields or {}
        if dim_copy.isAggregatable == nil then
            dim_copy.isAggregatable = true      -- defaults isAggregatable to true
        end
        dim_copy.domain = dim_copy.domain or name
        configuration[name] = dim_copy
    end
    return configuration
end

function M.build_search_provider_config(dimensions, searchProviderTemplates)
    local configuration = {}
    for name, dimension in pairs(dimensions) do
        local template = searchProviderTemplates[dimension.searchProvider]
        local domain = dimension.domain or name
        if configuration[domain] then
            assert(configuration[domain] == template,
                    "Found contradicting searchProvider config with the same domain name: "
                    .. domain)
        else
            configuration[domain] = template
        end
    end
    return configuration
end

M.keyValueStores = {
    memory = "com.yahoo.bard.webservice.data.dimension.MapStore",
    redis = "com.yahoo.bard.webservice.data.dimension.RedisStore"
}

return M
