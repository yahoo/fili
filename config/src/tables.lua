--
-- Created by IntelliJ IDEA.
-- User: ylin08
-- Date: 7/5/18
-- Time: 4:52 PM
-- To change this template use File | Settings | File Templates.
--

local utils = require("src/tableUtils")
local parser = require("src/jsonParser")

local M = {
    physicalTables = {},
    logicalTables = {}
}

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


-- table name = {description, metrics, dimensions, granuality}
PHYSICALTABLES = {
    wikiticker = {nil, {"added", "delta", "deleted"}, DEFAULT.ALL_DIM, "HOUR"}
}

-- table name = {description, metrics, dimensions, granuality, physicaltable}
LOGICALTABLES = {
    WIKIPEDIA = {nil, {"count", "added", "delta", "deleted"}, DEFAULT.ALL_DIM, {"ALL", "HOUR", "DAY"}, {"wikiticker"}},
    logicalTableTesterOne = {nil, DEFAULT.ALL_METRICS, DEFAULT.ALL_DIM, {"ALL", "HOUR", "DAY"}, {"wikiticker"}}
}

utils.add_phy_tables(PHYSICALTABLES, M.physicalTables)
utils.add_log_tables(LOGICALTABLES, M.logicalTables)

parser.save("TableConfigTemplate.json", M)
