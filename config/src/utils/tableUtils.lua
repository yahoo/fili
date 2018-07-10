--
-- Created by IntelliJ IDEA.
-- User: ylin08
-- Date: 7/5/18
-- Time: 5:06 PM
-- To change this template use File | Settings | File Templates.
--

local M = {}

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

