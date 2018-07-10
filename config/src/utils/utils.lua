--
-- Created by IntelliJ IDEA.
-- User: ylin08
-- Date: 7/3/18
-- Time: 11:42 AM
-- To change this template use File | Settings | File Templates.
--

local M = {}

M.print_table = function(arr, indentLevel)
    local str = ""
    local indentStr = "#"

    if(indentLevel == nil) then
        print(M.print_table(arr, 0))
        return
    end

    for i = 0, indentLevel do
        indentStr = indentStr.."\t"
    end

    for index,value in pairs(arr) do
        if type(value) == "table" then
            str = str..indentStr..index..": \n"..M.print_table(value, (indentLevel + 1))
        else
            str = str..indentStr..index..": "..value.."\n"
        end
    end
    return str
end

return M

