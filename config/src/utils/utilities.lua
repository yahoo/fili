--[[
-- A module containing miscellaneous utilities, like a shallow copy function.
--]]
local M = {}

--[[
-- Performs a shallow copy of the specified table
--
-- @param table The table to make a shallow copy of 
-- @return A shallow copy of the passed in table
--]]
function M.shallow_copy(table)
    local copy = {}
    for key, value in pairs(table) do
        copy[key] = value
    end
    return copy
end

return M
