-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

--[[
This module serves as the entry point for configuration generation. It pulls in
the dimensions, metrics and tables configuration defined in the dimensions.lua,
metrics.lua, and tables.lua files respectively, and generates JSON files that
are then read by Fili at start up.
]]
local appResourcesDir = "../../src/main/resources/"
local testResourcesDir = "../../src/test/resources/"

local parser = require("utils.jsonParser")
local dimensionUtils = require("utils.dimensionUtils")
local metricsUtils = require("utils.metricUtils")
local tableUtils = require("utils.tableUtils")
local metricMakerUtils = require("utils.metricMakerUtils")

-- dimensions returns dimension configuration keyed on name.
local dimensions = require("dimensions")
-- searchProviderTemplate returns preconfigured templates keyed on name.
local searchProviderTemplates = require("searchProviderTemplates")
-- keyValueStoreTemplates returns preconfigured templates keyed on name.
local keyValueStoreTemplates = require("keyValueStoreTemplates")
-- metrics returns metric configuration keyed on name.
local metrics = require("metrics")
local metricMakers = require("metricMakers")
-- tables returns a table containing two keys:
--  physical - A table of physical table configuration keyed on name
--  logical - A table of logical table configuration keyed on name
local tables = require("tables")

local dimensionConfig = dimensionUtils.build_dimensions_config(dimensions)
local searchProviderConfig = dimensionUtils.build_search_provider_config(dimensions, searchProviderTemplates)
local keyValueStoreConfig = dimensionUtils.build_key_value_store_config(dimensions, keyValueStoreTemplates)
local metricConfig = metricsUtils.build_metric_config(metrics)
local metricMakerConfig = metricMakerUtils.build_metric_maker_config(metricMakers)
local physicalTableConfig, logicalTableConfig = tableUtils.build_table_config(tables)

--- write to testResource/tableName
local function write_table(targetDir, tableName, configTable)
    parser.save(targetDir .. tableName, configTable)
    print(targetDir .. " table " .. tableName .. " Updated now.")
end

local function config(tableName, configTable)
    write_table(testResourcesDir, tableName, configTable)
    write_table(appResourcesDir, tableName, configTable)
end

--- a named map of all config, keyed on the target file name.
local tableNames = {
    ["DimensionConfig.json"] = dimensionConfig,
    ["KeyValueStoreConfig.json"] = keyValueStoreConfig,
    ["SearchProviderConfig.json"] = searchProviderConfig,
    ["MetricConfig.json"] = metricConfig,
    ["PhysicalTableConfig.json"] = physicalTableConfig,
    ["LogicalTableConfig.json"] = logicalTableConfig,
    ["MetricMakerConfig.json"] = metricMakerConfig
}

-- make the directory that is used for the test, which can be automated in
-- a script since creating a dir in Lua is awkard, in future.
-- can be circumvented by using the LuaFileSystem module
os.execute("mkdir -p " .. testResourcesDir)

-- add to the test/resource
for tableName, configTable in pairs(tableNames) do
    config(tableName, configTable)
end
