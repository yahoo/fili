-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

local defaults = require(LUTHIER_CONFIG_DIR .. 'defaults')
local dimension_defaulting = defaults.dimension_defaulting

--- A module that provides util functions for dimension config.
--
-- Furthermore, it provides two tables:
--  searchProviders - A mapping from the name of each search provider to its
--      full Java class name
--  keyValueStores - A mapping from the name of each keyValueStore to its
--      full Java class name
-- @module dimensionUtils

local M = {}
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
    local args = { ... }
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
--- the defaulting strategy in defaultingUtils is used for fields that is missing.
--
-- @param dimensions  A group of dimensions
-- @return dimension config, ready for parsing into json
function M.build_dimensions_config(dimensions)
    local configuration = {}
    for dimension_name, dimension in pairs(dimensions) do
        configuration[dimension_name] = dimension_defaulting(dimension_name, dimension)
    end
    return configuration
end

function M.build_key_value_store_config(dimensions, keyValueStoreTemplates)
    local configuration = {}
    for name, dimension in pairs(dimensions) do
        local template = keyValueStoreTemplates[dimension.keyValueStore]
        local dimensionDomain = dimension.dimensionDomain or name
        if configuration[dimensionDomain] then
            assert(configuration[dimensionDomain] == template,
                    "Found contradicting keyValueStore config with the same domain name: "
                            .. dimensionDomain)
        else
            configuration[dimensionDomain] = template
        end
    end
    return configuration
end

function M.build_search_provider_config(dimensions, searchProviderTemplates)
    local configuration = {}
    for name, dimension in pairs(dimensions) do
        local template = searchProviderTemplates[dimension.searchProvider]
        local dimensionDomain = dimension.dimensionDomain or name
        if configuration[dimensionDomain] then
            assert(configuration[dimensionDomain] == template,
                    "Found contradicting searchProvider config with the same domain name: "
                            .. dimensionDomain)
        else
            configuration[dimensionDomain] = template
        end
    end
    return configuration
end

return M
