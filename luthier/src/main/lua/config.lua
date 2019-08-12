-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

--[[
This module serves as the entry point for configuration generation. It pulls in
the dimensions, metrics and tables configuration defined in the dimensions.lua,
metrics.lua, and tables.lua files respectively, and generates JSON files that
are then read by Fili at start up.
]]

local CMD_USAGE = [[
--- command line usage:

--- `lua config.lua <DIR>` will set LUTHIER_CONFIG_DIR to <DIR>;
--- `lua config.lua` will set LUTHIER_CONFIG_DIR to "app" by default;
--- all other formats of command line argument are illegal.

--- This script builds json using the application level config lua files specified in LUTHIER_CONFIG_DIR.
--- directly running this script generates the json files into
--- luthier/src/main/lua/generated
]]

if #arg == 1 then
    LUTHIER_CONFIG_DIR = arg[1]
    if LUTHIER_CONFIG_DIR[-1] ~= '/' then
        LUTHIER_CONFIG_DIR = LUTHIER_CONFIG_DIR .. '/'
    end
elseif #arg == 0 then
    LUTHIER_CONFIG_DIR = "app/"
else
    print(CMD_USAGE)
    os.exit(-1)
end

local TEST_RESOURCE_DIR = "../../test/resources/"
local APP_RESOURCE_DIR = "../resources/"

--- general lua dependency
local json = require("lib/json")

--- application specific lua dependency
local function require_from_app_config(dirName)
    return require(LUTHIER_CONFIG_DIR .. dirName)
end
-- dimensions returns dimension configuration keyed on name.
local dimensions = require_from_app_config("dimensions")
-- searchProviderTemplate returns preconfigured templates keyed on name.
local searchProviderTemplates = require_from_app_config("searchProviderTemplates")
-- keyValueStoreTemplates returns preconfigured templates keyed on name.
local keyValueStoreTemplates = require_from_app_config("keyValueStoreTemplates")
-- metrics returns metric configuration keyed on name.
local metrics = require_from_app_config("metrics")
local metricMakers = require_from_app_config("metricMakers")
-- tables returns a Lua-Table containing two keys:
--  physical - A table of physical table configuration keyed on name
--  logical - A table of logical table configuration keyed on name
local tables = require_from_app_config( "tables")
-- *Utils are modules to help us compile the final json
local dimensionUtils = require_from_app_config("utils/dimensionUtils")
local metricsUtils = require_from_app_config("utils/metricUtils")
local tableUtils = require_from_app_config("utils/tableUtils")
local metricMakerUtils = require_from_app_config("utils/metricMakerUtils")

local dimensionConfig = dimensionUtils.build_dimensions_config(dimensions)
local searchProviderConfig = dimensionUtils.build_search_provider_config(dimensions, searchProviderTemplates)
local keyValueStoreConfig = dimensionUtils.build_key_value_store_config(dimensions, keyValueStoreTemplates)
local metricConfig = metricsUtils.build_metric_config(metrics)
local metricMakerConfig = metricMakerUtils.build_metric_maker_config(metricMakers)
local physicalTableConfig, logicalTableConfig = tableUtils.build_table_config(tables)

--- write to testResource/entityName
local function write_config_entity(targetDir, entityName, configEntity)
    json.save(targetDir .. entityName, configEntity)
    print(targetDir .. entityName .. " Updated now.")
end

local function write_config(configEntityName, configEntity)
    write_config_entity(TEST_RESOURCE_DIR, configEntityName, configEntity)
    write_config_entity(APP_RESOURCE_DIR, configEntityName, configEntity)
end

--- a named map of all config, keyed on the target file name.
local entityNames = {
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
os.execute("mkdir -p " .. TEST_RESOURCE_DIR)
os.execute("mkdir -p " .. APP_RESOURCE_DIR)

-- add to the test/resource
for entityName, configEntity in pairs(entityNames) do
    write_config(entityName, configEntity)
end
