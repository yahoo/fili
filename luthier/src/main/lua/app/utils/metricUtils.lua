-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

local defaults = require(LUTHIER_CONFIG_DIR .. 'defaults')
local metric_defaulting = defaults.metric_defaulting

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
    local configuration = {}
    for metric_name, metric in pairs(metrics) do
        configuration[metric_name] = metric_defaulting(metric_name, metric)
    end
    return configuration 
end

return M
