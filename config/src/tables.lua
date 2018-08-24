-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

--[[
This is where we define tables for Fili. Physical tables contains dataset in
druid, with physical metrics and dimensions defined in druid's configuration
file. Logical tables are tables defined by users, every logical table has their
dependency physical table, api metric names that are references of metrics
definitions defined in metric config file, dimensions for this logical table,
and a set of available time granularity for this logical table.
]]

local M = {}

-------------------------------------------------------------------------------
-- Physical Tables
--[[
    Physical tables are defined in table M.physical that map physical table's
    name (should be the same as the dataset names defined in druid's config
    file) to a dictionary of physical table configuration:

    * name - The name of the physical table
        Defaults to the configuration's table key
    * description - Brief documentation about the physical table.
        Defaults to the configuration's table key
    * metrics - A list of names of metrics for this physical table, the name of
        a metric should be the same as is defined in druid's config file.
    * dimensions - A list of names of  dimensions for this physical table, the
        name of a dimension should be the ones defined in druid's config
        file.
    * granularity - The granularity of the physical table. This does NOT need
        to be the same as the granularity of the Druid table, though it is
        recommended. This is used to decide at which granularities the table
        can respond to queries. For example, a physical table configured for
        "day" won't be used to answer any queries at the hourly grain, even
        if the Druid table is at the hourly grain. Naturally, you'll get weird
        results if you configure a physical table for a more precise granularity
        than is supported by the backing dataset.
]]
-------------------------------------------------------------------------------

local wikipedia_dimensions = {
    "comment",
    "countryIsoCode",
    "regionIsoCode",
    "page",
    "user",
    "isUnpatrolled",
    "isNew",
    "isRobot",
    "isAnonymous",
    "isMinor",
    "namespace",
    "channel",
    "countryName",
    "regionName",
    "metroCode",
    "cityName"
}

local air_quality_dimensions = {
    "PT08.S2(NMHC)",
    "PT08.S4(NO2)",
    "PT08.S4(NO2)",
    "C6H6(GT)",
    "PT08.S1(CO)",
    "NOx(GT)",
    "RH",
    "AH",
    "NMHC(GT)",
    "T",
    "PT08.S3(NOx)",
    "PT08.S5(O3)",
    "CO(GT)"
}

M.physical = {
    wikiticker = {
        metrics = {
            "added",
            "delta",
            "deleted"
        },
        dimensions = wikipedia_dimensions,
        granularity = "hour"
    },
    air = {
        metrics = {
            "CO",
            "NO2"
        },
        dimensions = air_quality_dimensions,
        granularity = "hour"
    }
}

-------------------------------------------------------------------------------
-- Logical Tables
--[[
    Logical tables are defined in table M.logical. They map logical table
    names to their configuration:

    * name - The name of the table
        Defaults to the configuration's key
    * description - Brief documentation about the logical table.
        Defaults to the configuration's key
    * metrics - A set of API metrics' name for this logical table, these 
        should be a subset of the metrics configured in metrics.lua.
    * dimensions - A set of dimensions for this logical table, the set of
        dimensions should be a subset of dimensions in its dependent physical
        tables.
    * granularity - A group of available granularities of this logical table, the
       granularity can be "all", "hour", "day", "week", or "month."
    * physicaltables - A list of the names of the physical tables that this
        logical table depends on

    Logical tables serve two purposes:
        1. They provide a logical grouping of metrics and dimensions for people
            to query against.
        2. They serve as a means of grouping a physical table and its 
            performance slices together. A performance slice is a druid
            table that is intended to be a subtable of another druid table.
            Typically this table has fewer dimensions or preaggregated to a
            higher granularity. Since the table is smaller, it can typically
            answer queries faster, though it can answer fewer queries. Fili's
            inended design pattern is for each logical table to have a "base
            fact" physical table, which can answer *any* query that can be sent
            against the logical table, and a collection of performance slices
            that can quickly answer the most common queries.
]]
-------------------------------------------------------------------------------

M.logical = {
    wikipedia = {
        metrics =  {"count", "added", "delta", "deleted"},
        dimensions = wikipedia_dimensions,
        granularity = {"ALL", "HOUR", "DAY"},
        physicaltables = {"wikiticker"}
    },
    air_quality = {
        metrics = {"averageCOPerDay", "averageNO2PerDay"},
        dimensions = air_quality_dimensions,
        granularity = {"ALL", "HOUR", "DAY"},
        physicaltables = {"air"}
    }
}

return M
