-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

--- a module provides general util functions.
-- @module Utils

local M = {}

--- Print a lua table.
--
-- @param table  a lua table
-- @param indentLevel the indent level of lua table to print
M.print_table = function(table, indentLevel)
    local str = ""
    local indentStr = "#"

    if(indentLevel == nil) then
        print(M.print_table(table, 0))
        return
    end

    for i = 0, indentLevel do
        indentStr = indentStr.."\t"
    end

    for index,value in pairs(table) do
        if type(value) == "table" then
            str = str..indentStr..index..": \n"..M.print_table(value, (indentLevel + 1))
        else
            str = str..indentStr..index..": "..value.."\n"
        end
    end
    return str
end

--- Deep copy a lua table.
--
-- @param table the original table
-- @return a copy of table
M.clone = function(table)
    local lookup_table = {}
    local function copyObj(table)
        if type(table) ~= "table" then
            return table
        elseif lookup_table[table] then
            return lookup_table[table]
        end

        local new_table = {}
        lookup_table[table] = new_table
        for key, value in pairs(table) do
            new_table[copyObj(key)] = copyObj(value)
        end
        return setmetatable(new_table, getmetatable(table))
    end
    return copyObj(table)
end

return M

