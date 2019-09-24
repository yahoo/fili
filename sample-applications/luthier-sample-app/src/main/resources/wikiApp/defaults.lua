-- Copyright 2019 Oath Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

--- a module provides default strategies, including constants and variables, in a centralized way.
-- @module Utils

local misc = require 'lib.misc'

local M = {}

--- Constants in all lua files:
-- Dimensions
M.DEFAULT_DIMENSION_TYPE = "KeyValueStoreDimension"
M.DEFAULT_DIMENSION_CATEGORY = "UNKNOWN_CATEGORY"
M.DEFAULT_DIMENSION_IS_AGGREGATABLE = true
M.DEFAULT_DIMENSION_FIELDS = {}
M.DEFAULT_DIMENSION_KEY_VALUE_STORE = "memory"
M.DEFAULT_DIMENSION_SKIP_LOADING = false
M.DEFAULT_DIMENSION_STORAGE_STRATEGY = "LOADED"
-- Physical Tables
M.DEFAULT_PHYSICAL_TYPE = "strict"
M.DEFAULT_PHYSICAL_DEPENDENT_TABLE = {}
M.DEFAULT_PHYSICAL_DATE_TIME_ZONE = "UTC"
M.DEFAULT_PHYSICAL_GRANULARITY = "day"
M.DEFAULT_PHYSICAL_COLUMN_NAME_MAP = {}
-- Logical Tables
M.DEFAULT_LOGICAL_TYPE = "default"
M.DEFAULT_LOGICAL_CATEGORY = "GENERAL"
M.DEFAULT_LOGICAL_RETENTION = "P1Y"
M.DEFAULT_LOGICAL_DEPENDENT_TABLE = {}
M.DEFAULT_LOGICAL_DATE_TIME_ZONE = "UTC"
-- Metrics
M.DEFAULT_METRIC_TYPE = "default"
M.DEFAULT_METRIC_CATEGORY = "GENERAL"
M.DEFAULT_METRIC_DEPENDENCY_NAMES = {}
--- Defaulting methods:
-- Dimensions
function M.dimension_defaulting(dimension_name, dimension)
    local dim_config = misc.shallow_copy(dimension)
    dim_config.longName = dim_config.longName or dimension_name
    dim_config.description = dim_config.description or dim_config.longName   -- this line must follow the previous one
    dim_config.dimensionDomain = dim_config.dimensionDomain or dimension_name
    dim_config.type = dim_config.type or M.DEFAULT_DIMENSION_TYPE
    dim_config.category = dim_config.category or M.DEFAULT_DIMENSION_CATEGORY
    dim_config.defaultFields = dim_config.defaultFields or M.DEFAULT_DIMENSION_FIELDS
    dim_config.keyValueStore = dim_config.keyValueStore or M.DEFAULT_DIMENSION_KEY_VALUE_STORE
    if dim_config.isAggregatable == nil then
        dim_config.isAggregatable = M.DEFAULT_DIMENSION_IS_AGGREGATABLE
    end
    if dim_config.skipLoading == nil then
        dim_config.skipLoading = M.DEFAULT_DIMENSION_SKIP_LOADING
    end
    dim_config.storageStrategy = dim_config.storageStrategy or M.DEFAULT_DIMENSION_STORAGE_STRATEGY
    return dim_config
end

function M.physical_table_defaulting(physical_table_name, physical_table)
    local phys_config = misc.shallow_copy(physical_table)
    phys_config.description = phys_config.description or physical_table_name
    phys_config.type = phys_config.type or M.DEFAULT_PHYSICAL_TYPE
    phys_config.physicalTables = phys_config.physicalTables or M.DEFAULT_PHYSICAL_DEPENDENT_TABLE
    phys_config.dateTimeZone = phys_config.dateTimeZone or M.DEFAULT_PHYSICAL_DATE_TIME_ZONE
    phys_config.granularity = phys_config.granularity or M.DEFAULT_PHYSICAL_GRANULARITY
    phys_config.logicalToPhysicalColumnNames = phys_config.logicalToPhysicalColumnNames
            or M.DEFAULT_PHYSICAL_COLUMN_NAME_MAP
    return phys_config
end

function M.logical_table_defaulting(logical_table_name, logical_table)
    local logi_config = misc.shallow_copy(logical_table)
    logi_config.description = logi_config.description or logical_table_name
    logi_config.type = logi_config.type or M.DEFAULT_LOGICAL_TYPE
    logi_config.category = logi_config.category or M.DEFAULT_LOGICAL_CATEGORY
    logi_config.retention = logi_config.retention or M.DEFAULT_LOGICAL_RETENTION
    logi_config.longName = logi_config.longName or logical_table_name
    logi_config.description = logi_config.description or logi_config.longName  -- this line must follow the previous one
    logi_config.physicalTables = logi_config.physicalTables or M.DEFAULT_LOGICAL_DEPENDENT_TABLE
    logi_config.dateTimeZone = logi_config.dateTimeZone or M.DEFAULT_LOGICAL_DATE_TIME_ZONE
    return logi_config
end

function M.metric_defaulting(metric_name, metric)
    local metric_config = misc.shallow_copy(metric)
    metric_config.type = metric_config.type or M.DEFAULT_METRIC_TYPE
    metric_config.longName = metric_config.longName or metric_name
    metric_config.description = metric_config.description or metric_config.longName   -- this line must follow the previous one
    metric_config.category = metric_config.category or M.DEFAULT_METRIC_CATEGORY
    metric_config.dependencyMetricNames = metric_config.druidMetric and {metric_config.druidMetric}
            or metric_config.dependencies           -- if druidMetric exists: use {druidMetric}
            or M.DEFAULT_METRIC_DEPENDENCY_NAMES    -- else: if dependencies exists: use dependencies
                                                    --       else: use an empty table {}
    return metric_config
end
return M
