-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file
-- distributed with this work for terms.

--[[
This is where we define the metrics for Fili. Metrics are formulas (for
example, formulas of aggregations and post-aggregations that we send down to
Druid). These formulas are constructed by taking Makers (which can be thought
of as operators and operands) and combining them to create metrics.

For example, the ArithmeticMaker defines the operators +, -, /, so you
can think of the ArithmeticMaker as defining the bits _ + _, _ - _, _ * _,
and _ / _.

Meanwhile, the LongSumMaker defines basic longsums in Druid, which can be
thought of as a simple number when building them up with other Makers. So
bits of formulas like x or y.

We can build a metric by "building up" a formula using the ArithmeticMaker and
the LongSumMaker:

LongSumMaker("metric1") + LongSumMaker("metric2")

This is specified in Lua as:

METRICS = {
 metric1 = {maker="longSum", druidMetric="metric1"},
 metric2 = {maker="longSum", druidMetric="metric2"},
 sum = {maker="arithmeticPLUS", dependencies={"metric1", "metric2"}}
}

Naturally of course these can be nested like any formula. So we could add
the following to METRICS above:

METRICS = {
 metric1 = {maker="longSum"},
 metric2 = {maker="longSum"},
 sum = {maker="arithmeticPlus", dependencies={"metric1", "metric2"}}
 difference = {maker="arithmeticMinus", {"sum", "metric1"}}
}

So the metric "difference" is the (rather silly) formula:
(LongSum("metric1") + LongSum("metric2")) - LongSum("metric1")
--]]

-------------------------------------------------------------------------------
-- Metrics
--[[
    Metrics are formulas built from makers. They're defined
    in a table that maps names to metrics. Each metric is itself a table with
    the following keys:
        longName - A longer, more human friendly name for the metric. Defaults
            to the metric name.
        description - Short documentation about the metric. Defaults to the
            metric name
        maker - The name of the maker to use to define the metric.
        dependencies - A list of names of Fili metrics that this metric operates
            on, if any, only applies for nested metrics
        druidMetric - The name of the druid metric that this metric operates
            on directly.
--]]
-------------------------------------------------------------------------------
local M = {}

local metrics = {
    longSumCO = {
        maker = "longSum",
        druidMetric = "CO"
    },
    count = {
        maker = "count"
    },
    added = {
        maker = "doubleSum",
        druidMetric="added"
    },
    addedMax = {
        maker = "doubleMax",
        druidMetric="added"
    },
    addedMin = {
        maker = "doubleMin",
        druidMetric="added"
    },
    delta = {
        maker = "doubleSum",
        druidMetric="delta"
    },
    deltaMax = {
        maker = "longMax",
        druidMetric="delta"
    },
    deltaMin = {
        maker = "longMin",
        druidMetric="delta"
    },
    deleted = {
        maker = "doubleSum",
        druidMetric= "deleted"
    },
    COM = {
        maker = "doubleSum",
        druidMetric = "CO"
    },
    NO2M = {
        maker = "doubleSum",
        druidMetric = "NO2"
    },
    averageCOPerDay = {
        maker = "aggregationAverageByDay",
        dependencies = {"COM"}
    },
    averageNO2PerDay = {
        maker = "aggregationAverageByDay",
        dependencies = {"NO2M"}
    }
}

for metric_name, metric_content in pairs(metrics) do
    metric_content.type = "default"
    M[metric_name] = metric_content
end

return M
