-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

--[[
This module serves as the entry point for configuration generation. It pulls in
the dimensions, metrics and tables configuration defined in the dimensions.lua,
metrics.lua, and tables.lua files respectively, and generates JSON files that
are then read by Fili at start up.
]]

local parser = require("utils.jsonParser")
local dimensionUtils = require("utils.dimensionUtils")
local metricsUtils = require("utils.metricUtils")
local tableUtils = require("utils.tableUtils")

-- dimensions returns dimension configuration keyed on name.
local dimensions = require("dimensions")
-- metrics returns metric configuration keyed on name.
local metrics = require("metrics")
-- tables returns a table containing two keys:
--  physical - A table of physical table configuration keyed on name
--  logical - A table of logical table configuration keyed on name
local tables = require("tables")

local dimensionConfig = dimensionUtils.build_dimensions_config(dimensions)
local metricConfig = metricsUtils.build_metric_config(metrics)
local tableConfig = tableUtils.build_table_config(tables)

parser.save("../external/DimensionConfig.json", dimensionConfig)
parser.save("../external/MetricConfig.json", metricConfig)
parser.save("../external/TableConfig.json", tableConfig)
-- parser.save("../external/MakerConfig.json", require("makers"))
