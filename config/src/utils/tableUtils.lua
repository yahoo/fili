-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

--- a module provides util functions for table config.
-- @module tableUtils

local M = {}

--- Parse a group of physical tables config and add them into a table.
--
-- @param tables  A group of physical tables
-- @param t  The table for storing physical tables
function M.add_phy_tables(tables, t)
    for name, physical_table in pairs(tables) do
        table.insert(t, {
            name = name,
            description =physical_table[1] or physical_table.description,
            metrics = physical_table[2] or physical_table.metrics,
            dimensions = physical_table[3] or physical_table.dimensions,
            granularity = physical_table[4] or physical_table.granularity
        })
    end
end

--- Parse a group of logical tables config and add them into a table.
--
-- @param tables  A group of logical tables
-- @param t  The table for storing logical tables
function M.add_log_tables(tables, t)
    for name, logical_table in pairs(tables) do
        table.insert(t, {
            name = name,
            description = logical_table[1] or logical_table.description,
            apiMetricNames = logical_table[2] or logical_table.metrics,
            dimensions = logical_table[3] or logical_table.dimensions,
            granularity = logical_table[4] or logical_table.granularity,
            physicalTables = logical_table[5] or logical_table.physical_tables
        })
    end
end

return M

