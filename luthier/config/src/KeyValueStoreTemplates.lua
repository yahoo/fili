-- Copyright 2019 Oath Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

--- A module that provide config about each domain of keyValueStore, will be used by the dimensions.lua
--
-- For custom keyValueStore template, specify the search provider type in TYPE with fully qualified class name
-- when needed.
-- The corresponding arguments should go into the M map

local M = {}

-- If you used the default name resolution in LuthierIndustrialPark,
-- using this FULLY_QUALIFIED_NAME is not necessary.
-- (You can very well use any alias we provided or the custom ones you added)
-- This is just to provide an insight about which class you will actually invoke.
local FULLY_QUALIFIED_NAME = {
    map = "com.yahoo.bard.webservice.data.dimension.MapStore",
    redis = "com.yahoo.bard.webservice.data.dimension.RedisStore"
}

--- For RedisStore config
-- config arguments TO BE DETERMINED
--- For mapStore config
-- <currently there is no argument needed>
M = {
    redis = {
        type = FULLY_QUALIFIED_NAME.redis,
        -- to be completed
    },
    memory = {
        type = FULLY_QUALIFIED_NAME.map,
    },
}

return M
