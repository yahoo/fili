-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

--- A module that provide config about each domain of search provider, will be used by the dimensions.lua
--
-- For custom search provider template, specify the search provider type in TYPE with fully qualified class name
-- when needed.
-- The corresponding arguments should go into the M.templates

local M = {}

local FULLY_QUALIFIED_NAME = {
    lucene =
        "com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProvider",
    noop = "com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProvider",
    memory = "com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProvider"
}

--- For Lucene config
-- indexPath  Path to the lucene index files
-- maxResults  Maximum number of allowed results in a page
-- searchTimeout  Maximum time in milliseconds that a lucene search can run
--- For NoOp config
-- queryWeightLimit  Weight limit for the query, used as a cardinality approximation for this dimension
--- For Scan config
-- <currently there is no argument needed>
M = {
    lucene =  {
        type = FULLY_QUALIFIED_NAME.lucene,
        indexPath = "./target/tmp/lucene/",
        maxResults = 100000,
        searchTimeout = 600000
    },
    noOp = {
        type = FULLY_QUALIFIED_NAME.noOp,
        queryWeightLimit = 100000
    },
    memory = {
        type = FULLY_QUALIFIED_NAME.memory
    }
}

return M
