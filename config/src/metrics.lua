-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

--[[
This is where we define the metrics for Fili. Metrics in Fili are
effectively formulas (for example, formulas of aggregations and
post-aggregations that we send down to Druid). These formulas are constructed
by taking Makers (which, for the purposes of configuration, can be thought of
as operators and operands, i.e. bits of formulas) and combining them to create
metrics.

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
 metric1 = {maker=longSum, druidMetric="metric1"},
 metric2 = {maker=maker_dict.longSum, druidMetric="metric2"},
 sum = {maker=arithmeticPLUS, dependencies={"metric1", "metric2"}}
}

Naturally of course these can be nested like any formula. So we could add
the following to METRICS above:

METRICS = {
 metric1 = {maker=simpleMakers.longSum},
 metric2 = {maker=complexMakers.longSum},
 sum = {maker=complexMakers.arithmeticPlus, dependencies={"metric1", "metric2"}}
 difference = {maker=complexMakers.arithmeticMinus, {"sum", "metric1"}}
}

So the metric difference is the (rather silly) formula:
(LongSum("metric1") + LongSum("metric2")) - LongSum("metric1")
--]]

local M = {}
-------------------------------------------------------------------------------
-- Default
-------------------------------------------------------------------------------

local DEFAULT = {
    CLASS_BASE_PATH = "com.yahoo.bard.webservice.data.config.metric.makers."
}

-------------------------------------------------------------------------------
-- Makers
--[[
    Makers are templates for metrics. Most map directly to a single aggregation
    or post-aggregation in Druid (like the LongSumMaker for the longSum
    aggregation, or the ArithmeticMaker for the arithmetic post-aggregation).
    Others may be more complex and contain any number of complex aggregations
    and post-aggregations.

    Note that some makers are "simple" in that they don't depend on any other
    Fili metrics. For example, the LongSumMaker is simple, because it depends
    only on a metric in Druid (which it computes the longsum of). Meanwhile,
    the ArithmeticMaker is "complex" because it's computing the sum of two
    other Fili metrics. For example, it might compute the LongSum of metric1
    and the LongSum of metric2.

    Makers themselves are define in Java, as a part of your program using
    Fili. Therefore, all references to makers are fully-qualified Java class
    names.

    Each maker is a table containing the following keys:
        classPath: A fully qualified Java class name for the Maker that should
            be constructed
        params: A Jackon-like table that describes the parameters that should
            be sent to the constructor. If the constructor doesn't take any
            parameters, this field may be nil

    When building a custom maker, make sure to annotate its constructor with
    `JsonCreator`, and its parameters with `JsonParam` like you would to
    deserialize the object from JSON.
--]]
-------------------------------------------------------------------------------

-- A simpleMaker is a maker that depends only on a metric in Druid. For example,
-- the LongSumMaker, which computes the long sum of a metric in Druid.
local simpleMakers = {
    count = {
        classPath = DEFAULT.CLASS_BASE_PATH .. "CountMaker"
    },
    constant = {
        classPath = DEFAULT.CLASS_BASE_PATH .. "ConstantMaker"
    },
    longSum = {
        classPath = DEFAULT.CLASS_BASE_PATH .. "LongSumMaker"
    },
    doubleSum = {
        classPath = DEFAULT.CLASS_BASE_PATH .. "DoubleSumMaker"
    }
}

-- A complexMaker is a maker that depends on one or more other Fili metrics.
-- For example, the arithmeticPlus maker depends on two other Fili metrics.

local complexMakers = {}
for _, operation in ipairs({"PLUS","MINUS","MULTIPLY","DIVIDE"}) do
    complexMakers["arithmetic" .. operation] = {
        classPath = DEFAULT.CLASS_BASE_PATH .. "ArithmeticMaker",
        params = {
            ["function"] = operation
        }
    }
end

for _, grain in ipairs {"HOUR", "DAY"} do
    complexMakers["aggregateAverage" .. grain] = {
        classPath = DEFAULT.CLASS_BASE_PATH .. "AggregationAverageMaker",
        params = {
            innerGrain = grain
        }
    }
end

for orientation, flag in pairs {byRow=true, byColumn=false} do
    complexMakers["cardinal" .. orientation] = {
        classPath = DEFAULT.CLASS_BASE_PATH .. "CardinalityMaker",
        params = {
            byRow = flag
        }
    }
end

for sizeName, size in pairs {Big=4096, Medium=2048, Small=1024} do
    complexMakers["ThetaSketch" .. sizeName] = {
        classPath = DEFAULT.CLASS_BASE_PATH .. "ThetaSketch",
        params = {
            sketchSize=size
        }
    }
end

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
        maker - The maker to use to define the metric.
        dependencies - A list of names of Fili metrics that this metric operates
            on, if any
        druidMetric - The name of the druid metric that this metric operates
            on directly, if any
--]]
-------------------------------------------------------------------------------

return {
    count = {
        maker = simpleMakers.count
    },
    added = {
        maker = simpleMakers.doubleSum,
        druidMetric="added"
    },
    delta = {
        maker = simpleMakers.doubleSum,
        druidMetric="delta"
    },
    deleted = {
        maker = simpleMakers.doubleSum,
        druidMetric= "deleted"
    },
    COM = {
        maker = simpleMakers.doubleSum,
        druidMetric = "CO"
    },
    NO2M = {
        maker = simpleMakers.doubleSum,
        druidMetric = "NO2"
    },
    averageCOPerDay = {
        maker = complexMakers.aggregateAveragebyDay,
        dependencies = {"COM"}
    },
    averageNO2PerDay = {
        maker = complexMakers.aggregateAveragebyDay,
        dependencies = {"NO2M"}
    }
}
