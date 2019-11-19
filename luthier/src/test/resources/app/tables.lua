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
    * The table is keyed on the name of the physical table
    * description - Brief documentation about the physical table.
        Defaults to the configuration's table key
    * type - [Mandatory] The nature of this physical table, used to build the table internally.
        Currently supports
            * strict - strict single data source physical table:
                A physical table backed up by one druid table. When querying a strict table with
                partial data turned on, the data is considered "complete" if and only if there
                is no missing data in any of the queried columns.
            * permissive - permissive single data source physical table:
                A physical table backed up by one druid table. When querying a permissive table
                with partial data turned on, the data is considered "complete" if at least one
                of the queried columns has no missing data.
    * dateTimeZone - A case sensitive name according to joda's dateTimeZone to indicate the
        which zone this physical table belongs in. See further:
        https://www.joda.org/joda-time/timezones.html
        Defaults to "UTC"
    * granularity - the case-insensitive name for the physical table's granularity.
        Including:
            * year
            * month
            * day
            * hour
            * all
        Defaults to "day"
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
    * logicalToPhysicalColumnNames - used internally to supply a translation
        for Druid look up. In the example:
            * logiName = physiName
        logiName should refer to a name in this table's dimensions field;
        while physiName is the name used in Druid.
        Note: for more information how this config is prepared, please see
        the documentation in utils/tableUtils.lua
    * physicalTables - A list of the names of the physical tables that this
        logical table depends on
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
        dateTimeZone = "UTC",
        type = "strict",
        metrics = {
            "added",
            "addedMax",
            "addedMin",
            "delta",
            "deltaMax",
            "deltaMin",
            "deleted"
        },
        dimensions = wikipedia_dimensions,
        granularity = "hour",
        logicalToPhysicalColumnNames = {
            testDimension = "testDimensionPhysicalName"
        }
    },
    air = {
        type = "permissive",
        dateTimeZone = "UTC",
        metrics = {
            "COM",
            "NO2M"
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

    * The table is keyed on the name of the logical table
    * type - The nature of this logical table, used to build the table internally.
        Currently supports:
            * default - A simple set of rules just enough to describe a logical table correctly.
                if 'default' is selected, the following fields will be used in the config:
                    * metrics
                    * dimensions
                    * granularities
                    * physicalTables
                    * dateTimeZone
                    * category
                    * longName
                    * retention
                    * description
                the following fields will be ignored:
                    * apiFilters
        Defaults to 'default'.
    * category - the category in which the logical table belongs.
        Defaults to 'GENERAL'
    * longName - a longer name for descriptive uses.
        Defaults to the configuration's key
    * description - Brief documentation about the logical table.
        Defaults to the configuration's longName
    * metrics - A set of API metrics' name for this logical table, these
        should be a subset of the metrics configured in metrics.lua;
        Defaults to an empty lua table.
    * dimensions - A set of dimensions for this logical table, the set of
        dimensions should be a subset of dimensions in its dependent physical
        tables.
        Defaults to an empty lua table.
    * dateTimeZone - A case sensitive name according to joda's dateTimeZone to indicate
        which time zone the table's underlying data source is collected in. See further:
        https://www.joda.org/joda-time/timezones.html
        Defaults to "UTC"
    * retention - a String metadata meant to tell external customers the retention policy
        for the underlying druid table(s). Not used internally by Fili.
        String is in the ISO 8601 Duration format, i.e. PnYnMnDTnHnMnS
        e.g. "P1Y2M10DT2H30M" which means 1 year 2 months 10 days and 2 hours 30 minutes.
        Defaults to "P1Y" which means 1 year
    * granularities - A group of available granularities of this logical table, the
        granularity can be "all", "hour", "day", "week", or "month".
        Defaults to an empty lua table.
    * physicalTables - A list of the names of the physical tables that this
        logical table depends on
        Defaults to an empty lua table.

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
        category = "wikipedia category",
        longName = "wikipedia logical table",
        description = "wikipedia description",
        retention = "P2Y",
        metrics =  {
            "added",
            "addedMax",
            "addedMin",
            "delta",
            "deltaMax",
            "deltaMin",
            "deleted"
        },
        dimensions = wikipedia_dimensions,
        granularities = {
            "all",
            "hour",
            "day"
        },
        physicalTables = {"wikiticker"},
        dateTimeZone = "UTC"
    },
    air_quality = {
        metrics = {
            "averageCOPerDay", "averageNO2PerDay"
        },
        dimensions = air_quality_dimensions,
        granularities = {
            "all",
            "hour",
            "day"
        },
        physicalTables = {"air"}
    }
}

return M
