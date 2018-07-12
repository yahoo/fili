-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

local utils = require("utils/tableUtils")
local parser = require("utils/jsonParser")

local M = {
    physicalTables = {},
    logicalTables = {}
}

-------------------------------------------------------------------------------
-- Default
-------------------------------------------------------------------------------

DEFAULT = {
    ALL_DIM = {
        "COMMENT",
        "COUNTRY_ISO_CODE",
        "REGION_ISO_CODE",
        "PAGE",
        "USER",
        "IS_UNPATROLLED",
        "IS_NEW",
        "IS_ROBOT",
        "IS_ANONYMOUS",
        "IS_MINOR",
        "NAMESPACE",
        "CHANNEL",
        "COUNTRY_NAME",
        "REGION_NAME",
        "METRO_CODE",
        "CITY_NAME"
    },
    ALL_METRICS ={
        "count",
        "added",
        "delta",
        "deleted",
        "averageAddedPerHour",
        "averageDeletedPerHour",
        "plusAvgAddedDeleted",
        "MinusAddedDelta",
        "cardOnPage",
        "bigThetaSketch"
    }
}

-------------------------------------------------------------------------------
-- Physical Tables
-------------------------------------------------------------------------------

-- table name = {description, metrics, dimensions, granuality}
PHYSICALTABLES = {
    wikiticker = {nil, {"added", "delta", "deleted"}, DEFAULT.ALL_DIM, "HOUR"}
}

-------------------------------------------------------------------------------
-- Logical Tables
-------------------------------------------------------------------------------

-- table name = {description, metrics, dimensions, granuality, physicaltable}
LOGICALTABLES = {
    WIKIPEDIA = {nil, {"count", "added", "delta", "deleted"}, DEFAULT.ALL_DIM, {"ALL", "HOUR", "DAY"}, {"wikiticker"}},
    logicalTableTesterOne = {nil, DEFAULT.ALL_METRICS, DEFAULT.ALL_DIM, {"ALL", "HOUR", "DAY"}, {"wikiticker"}}
}

utils.add_phy_tables(PHYSICALTABLES, M.physicalTables)
utils.add_log_tables(LOGICALTABLES, M.logicalTables)

parser.save("../TableConfig.json", M)
