-- Copyright 2019 Oath Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

--- a module provides util functions for metricMaker config.
-- @module tableUtils

local M = {}

local misc = require 'utils./misc'

-------------------------------------------------------------------------------
-- Build Config
-------------------------------------------------------------------------------

--- Prepare a specific physical table for Fili to consume.
function M.build_metric_maker_config(metric_maker)
    local copy = misc.shallow_copy(metric_maker)
    return copy
end

return M
