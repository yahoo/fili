--
-- Created by IntelliJ IDEA.
-- User: ylin08
-- Date: 7/3/18
-- Time: 3:22 PM
-- To change this template use File | Settings | File Templates.
--

local utils = require("src/metricUtils")
local parser = require("src/jsonParser")
local u = require("src/utils")

local M = {
    makers = {},
    metrics = {}
}

local maker_dict = {}

DEFAULT = {
    CLASS = "com.yahoo.bard.webservice.data.config.metric.makers."
}

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

u.print_table(M.metrics)

utils.add_makers(utils.cache_makers, maker_dict)
utils.clean_cache_makers()

utils.add_all_makers(maker_dict, M.makers)
parser.save("MetricConfigTemplate.json", M)

return M

