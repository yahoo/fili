-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

--- a module provides util functions for table config.
-- @module tableUtils

local M = {}

local misc = require 'utils./misc'

-------------------------------------------------------------------------------
-- Build Config
-------------------------------------------------------------------------------

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
        local copy = misc.shallow_copy(physical_table)
        -- hywical table name pull up
        copy.type = copy.type or "strict"
        copy.description = copy.description or name
        copy.physicalTables = copy.physicalTables or {}
        copy.dateTimeZone = copy.dateTimeZone or "UTC"
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
        physical[copy.name or name] = copy
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
