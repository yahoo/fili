-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

--- a module provides util functions for table config.
-- @module tableUtils

local M = {}

local misc = require 'util/misc'

-------------------------------------------------------------------------------
-- Build Config
-------------------------------------------------------------------------------

--- Add physical table configs and logical table configs into table t.
--
-- @param tables A table containing two keys: 
--  physical - A table of physical table configuration, keyed on name
--  logical - A table of logical table configuration, keyed on name
-- @return The table for storing table configs and ready be parsed into json
function M.build_table_config(tables)

    local configuration = {
        physical = {},
        logical = {}
    }

    for name, physical_table in pairs(tables.physical) do
        local copy = misc.shallow_copy(physical_table)
        copy.name = copy.name or name
        copy.description = copy.description or name
        table.insert(configuration.physical, copy)
    end

    for name, logical_table in pairs(tables.logical) do
        local copy = misc.shallow_copy(logical_table)
        copy.name = copy.name or name
        copy.description = copy.description or name
        table.insert(configuration.logical, copy)
    end
    return t
end

return M
