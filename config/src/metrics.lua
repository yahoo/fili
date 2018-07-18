-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

local metrics_utils = require("utils.metricUtils")
local parser = require("utils.jsonParser")



local M = {
    makers = {},
    metrics = {}
}

local maker_dict = {}

-------------------------------------------------------------------------------
-- Default
-------------------------------------------------------------------------------

DEFAULT = {
    CLASS_BASE_PATH = "com.yahoo.bard.webservice.data.config.metric.makers."
}

-------------------------------------------------------------------------------
-- Makers
-------------------------------------------------------------------------------

-- makerName = {classPath, parameters}
DEFAULT_MAKERS = {
    count = {DEFAULT.CLASS_BASE_PATH .. "CountMaker"},
    constant = {DEFAULT.CLASS_BASE_PATH .. "ConstantMaker"},
    longSum = {DEFAULT.CLASS_BASE_PATH .. "LongSumMaker"},
    doubleSum = {DEFAULT.CLASS_BASE_PATH .. "DoubleSumMaker"}
}

-- makerName = {classPath, {parameter's name = {parameters, suffix}}}
COMPLEX_MAKERS = metrics_utils.generate_makers(
    {
        arithmetic = {
            DEFAULT.CLASS_BASE_PATH .. "ArithmeticMaker",
            {_function = {{"PLUS","MINUS","MULTIPLY","DIVIDE"}, {"PLUS", "MINUS","MULTIPLY","DIVIDE"}}}
        },
        aggregateAverage = {
            DEFAULT.CLASS_BASE_PATH .. "AggregationAverageMaker",
            {innerGrain = {{"HOUR", "DAY"}, {"byHour", "byDay"}}}
        },
        cardinal = {
            DEFAULT.CLASS_BASE_PATH .. "CardinalityMaker",
            {byRow = {{"true", "false"}, {"byRow", "byColumn"}}}
        },
        ThetaSketch = {
            DEFAULT.CLASS_BASE_PATH .. "ThetaSketchMaker",
            {sketchSize= {{"4096", "2048", "1024"}, {"Big", "Medium", "Small"}}}
        }
    }
)

metrics_utils.add_makers(DEFAULT_MAKERS, maker_dict)
metrics_utils.add_makers(COMPLEX_MAKERS, maker_dict)

-------------------------------------------------------------------------------
-- Metrics
-------------------------------------------------------------------------------

-- metric's name = {longName, description, maker, dependency metrics}
M.metrics = metrics_utils.generate_metrics(
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
        inlineMakerMetric = {nil, nil, {DEFAULT.CLASS_BASE_PATH .. "ThetaSketchMaker", {sketchSize = "4096"}}, {"page"}}
    }
)

metrics_utils.add_makers(metrics_utils.cache_makers, maker_dict)
metrics_utils.clean_cache_makers()

metrics_utils.insert_makers_into_table(maker_dict, M.makers)
parser.save("../MetricConfig.json", M)

return M
