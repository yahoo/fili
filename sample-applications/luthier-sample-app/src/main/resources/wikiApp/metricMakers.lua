-- Copyright 2019 Verizon Media Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file
-- distributed with this work for terms.
local M = {}

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
    Fili.

    Each maker is a table containing at least the following key:
        type: A unique string identifying the type for this maker. The full
        Java classname is always a good choice.
    It may also have additional keys containing whatever information is needed
    to build this maker.
--]]
-------------------------------------------------------------------------------

-- A simpleMaker is a maker that depends only on a metric in Druid. For example,
-- the LongSumMaker, which computes the long sum of a metric in Druid.
M.count = {
    type = "com.yahoo.bard.webservice.data.config.metric.makers.CountMaker"
}
M.constant = {
    type = "com.yahoo.bard.webservice.data.config.metric.makers.ConstantMaker"
}
M.longSum = {
    type = "com.yahoo.bard.webservice.data.config.metric.makers.LongSumMaker"
}
M.doubleSum = {
    type = "com.yahoo.bard.webservice.data.config.metric.makers.DoubleSumMaker"
}

-- A complexMaker is a maker that depends on one or more other Fili metrics.
-- For example, the arithmeticPlus maker depends on two other Fili metrics.

for _, operation in ipairs({"PLUS","MINUS","MULTIPLY","DIVIDE"}) do
    M["arithmetic" .. operation] = {
        type = "com.yahoo.bard.webservice.data.config.metric.makers.ArithmeticMaker",
        operation = operation
    }
end

for _, grain in ipairs {"Hour", "Day"} do
    M["aggregationAverageBy" .. grain] = {
        type = "com.yahoo.bard.webservice.data.config.metric.makers.AggregationAverageMaker",
        innerGrain = grain
    }
end

for orientation, flag in pairs {byRow=true, byColumn=false} do
    M["cardinal" .. orientation] = {
        type = "com.yahoo.bard.webservice.data.config.metric.makers.CardinalityMaker",
        byRow = flag
    }
end

for sizeName, size in pairs {Big=4096, Medium=2048, Small=1024} do
    M["thetaSketch" .. sizeName] = {
        type = "com.yahoo.bard.webservice.data.config.metric.makers.ThetaSketchMaker",
        sketchSize=size
    }
end

return M
