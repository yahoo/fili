-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

local utils = require("utils/metricUtils")
local parser = require("utils/jsonParser")
local u = require("utils/utils")

local M = {
    makers = {},
    metrics = {}
}

local maker_dict = {}

-------------------------------------------------------------------------------
-- Default
-------------------------------------------------------------------------------

DEFAULT = {
    CLASS = "com.yahoo.bard.webservice.data.config.metric.makers."
}

-------------------------------------------------------------------------------
-- Makers
-------------------------------------------------------------------------------

-- makerName = {classPath, parameters}
DEFAULT_MAKERS = {
    count = {DEFAULT.CLASS .. "CountMaker"},
    constant = {DEFAULT.CLASS .. "ConstantMaker"},
    longSum = {DEFAULT.CLASS .. "LongSumMaker"},
    doubleSum = {DEFAULT.CLASS .. "DoubleSumMaker"}
}

-- makerName = {classPath, parameters, suffix}
COMPLEX_MAKERS = utils.generate_makers(
    {
        arithmetic = {
            DEFAULT.CLASS .. "ArithmeticMaker",
            {_function = {"PLUS","MINUS","MULTIPLY","DIVIDE"}},
            {_function = {"PLUS", "MINUS","MULTIPLY","DIVIDE"}}
        },
        aggregateAverage = {
            DEFAULT.CLASS .. "AggregationAverageMaker",
            {innerGrain = {"HOUR", "DAY"}},
            {innerGrain = {"byHour", "byDay"}}
        },
        cardinal = {
            DEFAULT.CLASS .. "CardinalityMaker",
            {byRow = {"true", "false"}},
            {byRow = {"byRow", "byColumn"}}
        },
        ThetaSketch = {
            DEFAULT.CLASS .. "ThetaSketchMaker",
            {sketchSize= {"4096", "2048", "1024"}},
            {sketchSize = {"Big", "Medium", "Small"}}
        }
    }
)

utils.add_makers(DEFAULT_MAKERS, maker_dict)
utils.add_makers(COMPLEX_MAKERS, maker_dict)

-------------------------------------------------------------------------------
-- Metrics
-------------------------------------------------------------------------------

-- metric's name = {longName, description, maker, dependency metrics}
M.metrics = utils.generate_metrics(
    {
        COUNT = {nil, nil, maker_dict.count, nil},
        ADDED = {nil, nil, maker_dict.doubleSum, {"ADDED"}},
        DELTA = {nil, nil, maker_dict.doubleSum, {"DELTA"}},
        DELETED = {nil, nil, maker_dict.doubleSum, {"DELETED"}},
        averageAddedPerHour = {nil, nil, maker_dict.aggregateAveragebyHour, {"added"}},
        averageDeletedPerHour = {nil, nil, maker_dict.aggregateAveragebyHour, {"deleted"}},
        plusAvgAddedDeleted = {nil, nil, maker_dict.arithmeticPLUS, {"averageAddedPerHour", "averageDeletedPerHour"}},
        MinusAddedDelta = {nil, nil, maker_dict.arithmeticMINUS, {"added", "delta"}},
        cardOnPage = {nil, nil, maker_dict.cardinalbyRow, {"page"}},
        bigThetaSketch = {nil, nil, maker_dict.ThetaSketchBig, {"page"}},
        inlineMakerMetric = {nil, nil, {DEFAULT.CLASS .. "ThetaSketchMaker", {sketchSize = "4096"}}, {"page"}}
    }
)

utils.add_makers(utils.cache_makers, maker_dict)
utils.clean_cache_makers()

utils.insert_makers_into_table(maker_dict, M.makers)
parser.save("../MetricConfig.json", M)

return M

