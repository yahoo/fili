--
-- Created by IntelliJ IDEA.
-- User: ylin08
-- Date: 7/2/18
-- Time: 3:27 PM
-- To change this template use File | Settings | File Templates.
--

local M = {}

function M.add_dimensions(dimensions, t)
    for name, dimension in pairs(dimensions) do
        table.insert(t, {
            apiName = name,
            longName = dimension[1] or name,
            description = dimension[2] or name,
            fields = dimension[3],
            category = dimension[4]
        })
    end
end

function M.add_fields(fields_set)
    for name, fields in pairs(fields_set) do
        for index, field in pairs(fields) do
            field.description = field.description or field.name
        end
        M.config.fieldSets[name] = fields
    end
end

function pk(name)
    return { name = name, tags = { "primaryKey" } }
end

function f(...)
    local args = { ... }
    local fields = {}
    for index, name in pairs(args) do
        table.insert(fields, { name = name })
    end
    return unpack(fields)
end

return M