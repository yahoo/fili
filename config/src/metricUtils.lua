--
-- Created by IntelliJ IDEA.
-- User: ylin08
-- Date: 7/3/18
-- Time: 1:50 PM
-- To change this template use File | Settings | File Templates.
--

local M = {
    cache_makers = {}
}

function M.add_makers(makers, t)
    for name, maker in pairs(makers) do
        t[name] = {
            name = maker.name or name,
            class = maker.class or maker[1],
            params = maker.params or maker[2]
        }
    end
end

function M.add_all_makers(makers, t)
    for name, maker in pairs(makers) do
        table.insert(t, {
            name = name,
            class = maker.class,
            params = maker.params
        })
    end
end

function M.generate_makers(makers)
    local t = {}
    for name, maker in pairs(makers) do
        local m = M.generate_maker(name, table.unpack(maker))
        for name, value in pairs(m) do
            t[name] = value
        end
    end
    return t
end

function M.generate_maker(baseName, baseClass, params, suffix)
    local makers = {}
    for para_type, para_value in pairs(params) do
        for index, value in pairs(para_value) do
            makers[baseName .. suffix[para_type][index]] = {
                class = baseClass,
                params = {[string.gsub(para_type,"^_","")] = value}
            }
        end
    end
    return makers
end

function M.generate_metrics(metrics)
    local t = {}
    for name, metric in pairs(metrics) do
        table.insert(t, {
            apiName = metric.name or name,
            longName = metric.longName or metric[1] or name,
            description = metric.description or metric[2] or metric.longName,
            maker = metric.maker or metric[3].name or M.generate_maker_name(metric[3]),
            dependencyMetricNames = metric.dependencies or metric[4]
        })
    end
    return t
end

function M.generate_maker_name(maker)
    local name = maker[1]
    for key, val in pairs(maker[2]) do
        name = name.."."..key
        name = name.."."..val
    end
    M.cache_makers[name] = {class = maker[1], params = maker[2]}
    return name
end

function M.clean_cache_makers()
    M.cache_makers = {}
end

return M