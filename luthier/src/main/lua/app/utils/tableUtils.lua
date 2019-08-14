-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

--- a module provides util functions for table config.
-- @module tableUtils

local M = {}

local defaults = require(LUTHIER_CONFIG_DIR .. 'defaults')
local physical_table_defaulting = defaults.physical_table_defaulting
local logical_table_defaulting = defaults.logical_table_defaulting

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
    local phys_config = physical_table_defaulting(name, physical_table)
    -- rebuilds the logicalToPhysicalColumnNames map with nicer format
    local intermediate_map = phys_config.logicalToPhysicalColumnNames
    phys_config.logicalToPhysicalColumnNames = {}
    for logical_name, physical_name in pairs(intermediate_map) do
        local name_pair = {
            logicalName = logical_name,
            physicalName = physical_name
        }
        table.insert(phys_config.logicalToPhysicalColumnNames, name_pair)
    end
    return phys_config
end

local function logical_table_build(name, logical_table)
    return logical_table_defaulting(name, logical_table)
end

--- Add physical table configs and logical table configs into a configuration.
--
-- @param tables A table containing two keys:
--  physical - A table of physical table configuration, keyed on name
--  logical - A table of logical table configuration, keyed on name
-- @return The physical table and logical table for storing table configs and ready be parsed into json
function M.build_table_config(tables)
    local physical = {}
    local logical = {}

    for name, physical_table in pairs(tables.physical) do
        physical[name] = physical_table_build(name, physical_table)
    end

    for name, logical_table in pairs(tables.logical) do
        logical[name] = logical_table_build(name, logical_table)
    end
    return physical, logical
end

return M
