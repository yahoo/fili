-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

--- a module provides util functions for table config.
-- @module tableUtils

local M = {}

local misc = require 'utils./misc'

-------------------------------------------------------------------------------
-- Build Config
-------------------------------------------------------------------------------

--- Prepare a specific physical table for Fili to consume.
--
-- In this function, defaulting and some reformatting are applied.
-- Most notably:
-- for each of the pair in logicalToPhysicalColumnNames, e.g.
--
-- logicalToPhysicalColumnNames = {
--     ...
--     testDimension = "testDimensionPhysicalName",
--     ...
-- }
--
-- the corresponding Json will be
--
-- "logicalToPhysicalColumnNames": [
--   ...
--   {
--     "logicalName": "testDimension"
--     "physicalName": "testDimensionPhysicalName",
--   },
--   ...
-- ]
--
-- this "weird" reformatting is done to avoid a gotcha since we don't want the Json
-- object to stay as an ArrayNode. If we instead naively translate this, then it
-- will be an ObjectNode, i.e. { "logiName": "physiName" }, when it is non-empty,
-- but an ArrayNode, i.e. [], when it is empty.
local physical_table_build = function(name, physical_table)
    local copy = misc.shallow_copy(physical_table)
    copy.type = copy.type or "strict"
    copy.description = copy.description or name
    copy.physicalTables = copy.physicalTables or {}
    copy.dateTimeZone = copy.dateTimeZone or "UTC"
    copy.granularity = copy.granularity or "day"
    local intermediate_map = copy.logicalToPhysicalColumnNames or {}
    -- rebuilds the logicalToPhysicalColumnNames map with nicer format
    copy.logicalToPhysicalColumnNames = {}
    for logical_name, physical_name in pairs(intermediate_map) do
        local name_pair = {
            logicalName = logical_name,
            physicalName = physical_name
        }
        table.insert(copy.logicalToPhysicalColumnNames, name_pair)
    end
    return copy
end

--- Add physical table configs and logical table configs into a configuration.
--
-- @param tables A table containing two keys:
--  physical - A table of physical table configuration, keyed on name
--  logical - A table of logical table configuration, keyed on name
-- @return The table for storing table configs and ready be parsed into json
function M.build_table_config(tables)

    local physical = {}
    local logical = {}

    for name, physical_table in pairs(tables.physical) do
        physical[name] = physical_table_build(name, physical_table)
    end

    for name, logical_table in pairs(tables.logical) do
        local copy = misc.shallow_copy(logical_table)
        copy.description = copy.description or name
        copy.physicalTables = copy.physicalTables or {}
        logical[copy.name or name] = copy
    end
    return physical, logical
end

return M
