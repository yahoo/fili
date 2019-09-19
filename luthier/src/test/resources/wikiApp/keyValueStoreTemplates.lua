-- Copyright 2019 Oath Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

--- Provides keyValueStore configuration. A key value store is a structure for performing point lookups of dimension rows,
-- while search providers provide more of an "index" of dimension rows. KeyValueStores need to be able to perform their
-- lookups very fast, because they are used to annotate (potentially very large) Druid results.

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
