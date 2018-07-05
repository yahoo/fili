--
-- Created by IntelliJ IDEA.
-- User: ylin08
-- Date: 7/2/18
-- Time: 3:06 PM
-- To change this template use File | Settings | File Templates.
--
local Json = require("src/json")

local JsonTable = {}

JsonTable.save = function(filepath, table)
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

JsonTable.load = function(filepath)
    local file = io.open(filepath, "r" )

    if file then
        local contents = file:read( "*a" )
        JsonTable = Json.decode(contents);
        io.close( file )
        return JsonTable
    end
    return nil
end

return JsonTable