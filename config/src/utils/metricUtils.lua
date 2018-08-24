-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

local misc = require 'utils/misc'

--- a module provides util functions for metrics and makers config.
-- @module metricUtils

local M = {}

-------------------------------------------------------------------------------
-- Build Config
-------------------------------------------------------------------------------
--- Generate a set of metrics based on metric's config.
--
-- This function assigns default values for apiName, longName, and description
-- if they don't already exist, and consolidates druidMetric and dependencies 
-- into one field: dependencyMetricNames
--
-- @param metrics  A set of metric config
-- @return A list of metrics
function M.build_metric_config(metrics)
    local metrics = {}
    for name, metric in pairs(metrics) do
        local copy = misc.shallow_copy(metric)
        copy.apiName = copy.apiName or name
        copy.longName = copy.longName or name
        copy.description = copy.description or name
        copy.dependencyMetricNames = copy.druidMetric 
            and {copy.druidMetric} 
            or copy.dependencies
        table.insert(metrics, copy)
    end
    return metrics 
end

return M
