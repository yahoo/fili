-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

--- A module containing miscellaneous utilities, like a shallow copy function.
-- @module Utils

local M = {}

--- Print a lua table.
--
-- @param table  a lua table
-- @param indentLevel the indent level of lua table to print
function M.print_table(table, indentLevel)
    local str = ""
    local indentStr = "#"

    if(indentLevel == nil) then
        print(M.print_table(table, 0))
        return
    end

    indentStr = indentStr .. string.rep("\t", indentLevel)

    for index,value in pairs(table) do
        if type(value) == "table" then
            str = str .. indentStr .. index .. ": \n" .. M.print_table(
                value,
                indentLevel + 1
            )
        else
            str = str .. indentStr .. index .. ": " .. value .. "\n"
        end
    end
    return str
end

--- Deep copy a lua table.
--
-- @param table the original table
-- @return a deep copy of table
function M.clone(table)
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

-- Performs a shallow copy of a table.
--
-- @param table the original table
-- @return A shallow copy of the table
function M.shallow_copy(table)
    local copy = {}
    for key, value in pairs(table) do
        copy[key] = value
    end
    return copy
end

return M
