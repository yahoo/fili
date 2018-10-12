-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

local Json = require("lib.json")

--- a module provides functions to parse json file or object.
-- @module jsonParser

local M = {}

--- Save a lua table to a json file.
--
-- @param filepath  A file for saving json
-- @param table A lua table to be saved as json file
-- @return the lua table
function M.save(filepath, table)
    local file = io.open(filepath, "w")

    if file then
        local contents = Json.encode(table)
        file:write(contents)
        io.close(file)
        return table
    else
        return nil
    end
end

--- Load json file into a lua table.
--
-- @param filepath  A file for loading json
-- @return a lua table parsed from json file
function M.load(filepath)
    local file = io.open(filepath, "r" )

    if file then
        local contents = file:read( "*a" )
        M = Json.decode(contents);
        io.close( file )
        return M
    end
    return nil
end

return M
