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
    WIKI_DIM = {
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
    AIR_DIM = {
        "PT08.S2(NMHC)",
        "PT08.S4(NO2)",
        "NO2(GT)",
        "C6H6(GT)",
        "PT08.S1(CO)",
        "NOx(GT)",
        "RH",
        "AH",
        "NMHC(GT)",
        "T",
        "PT08.S3(NOx)",
        "PT08.S5(O3)",
        "CO(GT)"
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
    wikiticker = {nil, {"added", "delta", "deleted"}, DEFAULT.WIKI_DIM, "HOUR" },
    air = {nil, {"CO", "NO2", "Temp", "relativeHumidity", "absoluteHumidity"}, DEFAULT.AIR_DIM, "HOUR" }
}

-------------------------------------------------------------------------------
-- Logical Tables
-------------------------------------------------------------------------------

-- table name = {description, metrics, dimensions, granuality, physicaltable}
LOGICALTABLES = {
    WIKIPEDIA = {nil, {"count", "added", "delta", "deleted"}, DEFAULT.WIKI_DIM, {"ALL", "HOUR", "DAY"}, {"wikiticker"}},
    logicalTableTesterOne = {nil, DEFAULT.ALL_METRICS, DEFAULT.WIKI_DIM, {"ALL", "HOUR", "DAY"}, {"wikiticker"}},
    air_logical = {nil, {"COM", "NO2M", "Temp", "AHM", "RHM"}, DEFAULT.AIR_DIM, {"ALL", "HOUR", "DAY"}, {"air"}}
}

table_utils.add_phy_tables(PHYSICALTABLES, M.physicalTables)
table_utils.add_log_tables(LOGICALTABLES, M.logicalTables)

parser.save("../external/TableConfig.json", M)
