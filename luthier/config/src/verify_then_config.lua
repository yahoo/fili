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
local referenceResources = testResourcesDir .. "snapshots/"
local snapshotHistoryFilePath = referenceResources .. ".snapshotsHistory.json"

local parser = require("utils.jsonParser")
local dimensionUtils = require("utils.dimensionUtils")
local metricsUtils = require("utils.metricUtils")
local metricMakerUtils = require("utils.metricMakerUtils")
local tableUtils = require("utils.tableUtils")

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
local physicalTableConfig, logicalTableConfig = tableUtils.build_table_config(tables)
local metricMakerConfig = metricMakerUtils.build_metric_maker_config(metricMakers)

--- gives the user the option to accept the change and make new snapshot
local function confirm_or_exit()
    print("If you want to accept the above change, press y to proceed to make the new snapshot")
    print("Otherwise press n to abort, the json files in the resources directory won't be changed.")
    local user_input = ""
    while user_input ~= "n" and user_input ~= "y" do
        print("try with n/y")
        user_input = io.read("*l")
    end
    if user_input == "n" then
        os.exit(-1)
    end
end

--- Obtained and modified from
--- https://web.archive.org/web/20131225070434/http://snippets.luacode.org/snippets/Deep_Comparison_of_Two_Values_3
--- returns:
---     isMatching - bool: true if two tables match
---     failMessage - string: a string that describes the reason of failure with a mismatching element, nil if matched;
---             should be one of the bottom level nonexistent key or mismatched value, if we view json as a tree.
local function deepCompareTable(ref_t, new_t)
    -- assert type of the recursive call to match
    if type(ref_t) ~= type(new_t) then
        return false, " the old type: [" .. type(ref_t) .. "] " .. tostring(ref_t) ..
                " and new type: [" .. type(new_t) .. "] " .. tostring(new_t) .. " don't match"
    end
    -- recursion base case comparison
    if type(ref_t) ~= "table" then
        if ref_t == new_t then return true, nil end
        return false, "the old value: " .. tostring(ref_t) .. " and new value: " .. tostring(new_t) .. " don't match"
    end
    -- put both old and new tables into arrays so we can compare them in a specific order.
    local ref_key_arr = {}
    local new_key_arr = {}
    for i, v in pairs(ref_t) do table.insert(ref_key_arr, i) end
    for i, v in pairs(new_t) do table.insert(new_key_arr, i) end
    table.sort(ref_key_arr)
    table.sort(new_key_arr)
    for _, ref_k in ipairs(ref_key_arr) do
        local ref_v = ref_t[ref_k]
        local new_v = new_t[ref_k]
        if new_v == nil then return false, "key: '" .. tostring(ref_k) .. "' does not exist in newly generated table" end
        local isMatching, erroneousKey = deepCompareTable(ref_v, new_v)
        if not isMatching then return false, tostring(ref_k) .. " > " .. erroneousKey end
    end
    for _, new_k in ipairs(new_key_arr) do
        local ref_v = ref_t[new_k]
        local new_v = new_t[new_k]
        if ref_v == nil then return false, "key: '" .. tostring(new_k) .. "' does not exist in the old table" end
        local isMatching, erroneousKey = deepCompareTable(ref_v, new_v)
        if not isMatching then return false, tostring(new_k) .. " > " .. erroneousKey end
    end
    return true, nil
end

--- Wrapper for the deepCompareTable
--- exit if not successful, will finish function execution if succeeded.
local function verify_tables(reference_table, new_table, table_name)
    if type(reference_table) ~= 'table' or type(new_table) ~= 'table' then
        print("check your " .. testResourcesDir .. table_name .. " and " ..
                referenceResources .. table_name .. ".\nAt least one of them is not a proper lua table.")
        confirm_or_exit()
    end
    local isMatching, failMessage = deepCompareTable(reference_table, new_table)
    if not isMatching then
        print("The table '" .. table_name .. "' does not match previous record.\n" ..
                "Mismatch reason: " .. failMessage ..
                "\nConfig process won't start.")
        confirm_or_exit()
    end
end

--- write to testResource/tableName
local function write_table(targetDir, tableName, configTable)
    parser.save(targetDir .. tableName, configTable)
    print("table " .. tableName .. " Updated now.")
end

--- Wrapper needed to build one table after checking the contents match with previous records.
local function verify_then_config(referenceDir, tableName, configTable)
    local referenceTable = parser.load(referenceDir .. tableName)
    verify_tables(referenceTable, configTable, tableName)
    io.write("Contents matches with previous record, ")
    write_table(testResourcesDir, tableName, configTable)
    write_table(appResourcesDir, tableName, configTable)
end

--- Yet another wrapper to operate on all tables.
--- will exit if any construction is not successful
local function verify_then_config_all(tableNames, snapshotTime)
    -- has a previous snapshot, do verification
    for tableName, configTable in pairs(tableNames) do
        if snapshotTime == nil then
            -- does not have a previous snapshot, directly config
            write_table(testResourcesDir, tableName, configTable)
        else
            verify_then_config(referenceResources .. snapshotTime, tableName, configTable)
        end
    end
    -- generate snapshot dir name based on current time, copy all files into corresponding directory
    local current_time = os.date("%Y_%m_%b_%d_%a_%X/")
    os.execute("mkdir -p " .. referenceResources .. current_time)
    for tableName, configTable in pairs(tableNames) do
        local newTable = parser.load(testResourcesDir .. tableName)
        parser.save(referenceResources .. current_time .. tableName, configTable)
    end
    print("new snapshot saved at " .. referenceResources .. current_time .. "*Config.json")
    return current_time
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
os.execute("mkdir -p " .. referenceResources)

--- if there has not been any record, return nil;
--- if there has been a .snapshotHistory.json, return the string representing the most recent config time.
--- also modifies the argument by populating with the existing json values in history_file
local function get_most_recent_snapshot_time(snapshot_history)
    local most_recent_snapshot_time = nil
    local history_file = io.open(snapshotHistoryFilePath,"r")
    print(snapshotHistoryFilePath)
    if history_file ~= nil then
        -- load previous history_file
        local old_history = parser.load(snapshotHistoryFilePath)
        for k,v in pairs(old_history) do snapshot_history[k] = v end
        most_recent_snapshot_time = snapshot_history[#snapshot_history]     -- gets the last string in this table
        io.close(history_file)
    else
        -- no .snapshotHistory.json exists yet, use new snapshot history
        print("No previous record found. Creating new snapshot history...")
    end
    return most_recent_snapshot_time
end

--- driver. Queries the referenceResources/.snapshotsHistory.json to look
--- for the most recent snapshot directory as reference *Config.json
local snapshot_history = {}
-- write the time of successful configuration into snapshot_history, then save it
local current_time = verify_then_config_all(tableNames, get_most_recent_snapshot_time(snapshot_history))
table.insert(snapshot_history, current_time)
parser.save(snapshotHistoryFilePath, snapshot_history)