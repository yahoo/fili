-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

--[[
This module serves as the entry point for configuration generation. It pulls in
the dimensions, metrics and tables configuration defined in the dimensions.lua,
metrics.lua, and tables.lua files respectively, and generates JSON files that
are then read by Fili at start up.
]]
local testResources = "../../src/test/resources/"

local parser = require("utils.jsonParser")
local dimensionUtils = require("utils.dimensionUtils")
local metricsUtils = require("utils.metricUtils")
local tableUtils = require("utils.tableUtils")

-- dimensions returns dimension configuration keyed on name.
local dimensions = require("dimensions")
-- searchProviderTemplate returns preconfigured templates keyed on name.
local searchProviderTemplates = require("searchProviderTemplates")
-- metrics returns metric configuration keyed on name.
local metrics = require("metrics")
-- tables returns a table containing two keys:
--  physical - A table of physical table configuration keyed on name
--  logical - A table of logical table configuration keyed on name
local tables = require("tables")

local dimensionConfig = dimensionUtils.build_dimensions_config(dimensions)
local searchProviderConfig = dimensionUtils.build_search_provider_config(dimensions, searchProviderTemplates)
local metricConfig = metricsUtils.build_metric_config(metrics)
local tableConfig = tableUtils.build_table_config(tables)

-- make the directory that is used for the test, which can be automated in
-- a script since creating a dir in Lua is awkard, in future.
-- can be circumvented by using the LuaFileSystem module
os.execute("mkdir -p " .. testResources)

-- add to the test/resource
parser.save(testResources .. "DimensionConfig.json", dimensionConfig)
parser.save(testResources .. "SearchProviderConfig.json", searchProviderConfig)
parser.save(testResources .. "MetricConfig.json", metricConfig)
parser.save(testResources .. "TableConfig.json", tableConfig)
-- parser.save("../../src/test/resource/MakerConfig.json", require("makers"))
