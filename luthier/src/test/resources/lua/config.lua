-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

--[[
This module serves as the entry point for configuration generation. It pulls in
the configuration for each concept in Fili, and consolidatest them into a single
table, with the following keys:

SearchProviderConfig - The configuration for search providers
KeyValueStoreConfig - The configuration for key value stores
MetricConfig - The configuration for metrics
MetricMakerConfig - The configuration for metric makers
PhysicalTableConfig - The configuration for physical tables 
LogicalTableConfig - The configuration for logical tables

The source of truth for these keys is com.yahoo.bard.webservice.data.config.luthier.ConceptType.
]]

LUTHIER_CONFIG_DIR = ""

local function appRequire(filename)
    return require(LUTHIER_CONFIG_DIR .. filename)
end

return function(app)
    LUTHIER_CONFIG_DIR = string.format("lua.%s.", app)
    -- *Utils are modules to help us compile the final json
    local dimensionUtils = appRequire "utils.dimensionUtils"
    local metricsUtils = appRequire "utils.metricUtils"
    local tableUtils = appRequire "utils.tableUtils"
    local metricMakerUtils = appRequire "utils.metricMakerUtils"

    -- dimensions returns dimension configuration keyed on name.
    local dimensions = appRequire "dimensions"

    -- searchProviderTemplate returns preconfigured templates keyed on name.
    local searchProviderTemplates = appRequire "searchProviderTemplates"

    -- keyValueStoreTemplates returns preconfigured templates keyed on name.
    local keyValueStoreTemplates = appRequire "keyValueStoreTemplates"

    -- metrics returns metric configuration keyed on name.
    local metrics = appRequire "metrics"
    local metricMakers = appRequire "metricMakers"

    -- tables returns a Lua-Table containing two keys:
    --  physical - A table of physical table configuration keyed on name
    --  logical - A table of logical table configuration keyed on name
    local tables = appRequire "tables"

    local physicalTableConfig, logicalTableConfig = tableUtils.build_table_config(tables)

    --- a map of all config, keyed on the concept name.
    return {
        DimensionConfig = dimensionUtils.build_dimensions_config(dimensions),
        KeyValueStoreConfig = dimensionUtils.build_key_value_store_config(dimensions, keyValueStoreTemplates),
        SearchProviderConfig = dimensionUtils.build_search_provider_config(dimensions, searchProviderTemplates),
        MetricConfig = metricsUtils.build_metric_config(metrics),
        PhysicalTableConfig = physicalTableConfig,
        LogicalTableConfig = logicalTableConfig,
        MetricMakerConfig = metricMakerUtils.build_metric_maker_config(metricMakers)
    }
end
