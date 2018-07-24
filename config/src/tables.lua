-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

local parser = require("utils.jsonParser")
local table_utils = require("utils.tableUtils")

local M = {
    physicalTables = {},
    logicalTables = {}
}

-------------------------------------------------------------------------------
-- Default
-------------------------------------------------------------------------------

DEFAULT = {
    ALL_DIM = {
        "comment",
        "countryIsoCode",
        "regionIsoCode",
        "page",
        "user",
        "isUnpatrolled",
        "isNew",
        "isRobot",
        "isAnonymous",
        "isMinor",
        "namespace",
        "channel",
        "countryName",
        "regionName",
        "metroCode",
        "cityName"
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

table_utils.add_phy_tables(PHYSICALTABLES, M.physicalTables)
table_utils.add_log_tables(LOGICALTABLES, M.logicalTables)

parser.save("../TableConfig.json", M)
