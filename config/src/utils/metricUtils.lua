-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

local utils = require("utils.utils")

--- a module provides util functions for metrics and makers config.
-- @module metricUtils

local M = {
    cache_makers = {}
}

-------------------------------------------------------------------------------
-- Makers
-------------------------------------------------------------------------------

--- Parse a group of config makers and add them into a table.
--
-- @param makers  A group of makers
-- @param t  The table for storing makers
function M.add_makers(makers, t)
    for name, maker in pairs(makers) do
        t[name] = {
            name = maker.name or name,
            classPath = maker.classPath or maker[1],
            params = maker.params or maker[2]
        }
    end
end

--- Insert a group of makers into a table.
--
-- @param makers  A group of makers
-- @param t  The table for storing makers
function M.insert_makers_into_table(makers, t)
    for name, maker in pairs(makers) do
        table.insert(t, {
            name = name,
            classPath = maker.classPath,
            params = maker.params
        })
    end
end

--- Generate a set of complex makers.
--
-- @param makers  A set of complex makers' config
-- @return t  The table for storing makers
function M.generate_makers(makers)
    local t = {}
    for name, maker in pairs(makers) do
        local m = M.generate_maker(name, maker[1], maker[2])
        for name, value in pairs(m) do
            t[name] = value
        end
    end
    return t
end

--- Generate a group of complex makers based on one maker's config.
--  maker's config example:
--  makerName = {
--    classPath,
--    {paramA = {AOne, ATwo}, {suffixAOne, suffixATwo}},
--    {paramB = {BOne, BTwo}, {suffixBOne, suffixBTwo}}
--  }
--  this config can be generated to a maker group contains four makers:
--  (1) makerNamesuffixAOnesuffixBOne = {class = classPath, params = {paramA = AOne, paramB = BOne}}
--  (2) makerNamesuffixAOnesuffixBTwo = {class = classPath, params = {paramA = AOne, paramB = BTwo}}
--  (3) makerNamesuffixATwosuffixBOne = {class = classPath, params = {paramA = ATwo, paramB = BOne}}
--  (4) makerNamesuffixATwosuffixBTwo = {class = classPath, params = {paramA = ATwo, paramB = BTwo}}
--
-- @param baseName The prefix name for these makers
-- @param baseClass  The class path for these makers
-- @param params_and_suffix All posible parameters and suffixes for these makers
-- @return a group of generated makers
function M.generate_maker(baseName, baseClass, params_and_suffix)

    local makers, p, s, n, i_n, r = {}, {}, {}, {}, {}, {}
    local params, suffix = {}, {}

    for name, value in pairs(params_and_suffix) do
        params[name] = value[1]
        suffix[name] = value[2]
    end

    for param_name, param_value in pairs(params) do
        local ss = string.gsub(param_name,"^_","")
        table.insert(p, param_value)
        table.insert(n, ss)
        i_n[ss] = #n
    end
    for param_name, suffix_value in pairs(suffix) do
        local ss = string.gsub(param_name,"^_","")
        s[i_n[ss]] = suffix_value
    end

    cartesian_calculator(p, s, n, 1, r, {}, "")

    for index, maker_info in pairs(r) do
        makers[baseName .. maker_info.suffix] = {
            classPath = baseClass,
            params = maker_info.params
        }
    end

    return makers
end

--- Clean maker cache
function M.clean_cache_makers()
    M.cache_makers = {}
end

--- Calculate the combinations of all parameters and store parameters combinations
-- and corresponding suffix into result.
--
-- @param params a list of parameters' value
-- @param suffix a list of suffix
-- @param names a list of parameters' name
-- @param start_index start index for recursion
-- @param result a table to store the result
-- @param p_c a subset of parameters for recursion purpose
-- @param s_c a subset of suffix for recursion purpose
--
-- generate a list of parameters combination:
-- e.g. {
--         {params = {paramA = AOne, paramB = BOne, paramC = COne},
--         suffix = suffixAOnesuffixBOnesuffixCOne},
--         ...
--      }
function cartesian_calculator(params, suffix, names, start_index, result, p_c, s_c)
    if (start_index == #params + 1) then do
        table.insert(result, {params = utils.clone(p_c), suffix = s_c})
        return
    end
    end
    for index, value in pairs(params[start_index]) do
        p_c[names[start_index]] = value
        cartesian_calculator(params, suffix, names, start_index + 1,
            result, p_c, s_c .. suffix[start_index][index])
    end
end

--- Generate a maker name for an inline maker,
-- (store generated maker into cache)
-- for example, inline maker:
--    {classpath, {paramsA = A, paramsB = B}}
-- would be generated as:
-- classpath.paramsA.A.paramsB.B
--
-- @param maker An inline maker defined in metric's config
-- @return A name for this maker
function generate_maker_name(maker)
    local name = maker[1]
    for key, val in pairs(maker[2]) do
        name = name.."."..key
        name = name.."."..val
    end
    M.cache_makers[name] = {classPath = maker[1], params = maker[2]}
    return name
end

-------------------------------------------------------------------------------
-- Metrics
-------------------------------------------------------------------------------

--- Generate a set of metrics based on metric's config.
--
-- @param metrics  A set of metric config
-- @return A list of metrics
function M.generate_metrics(metrics)
    local t = {}
    for name, metric in pairs(metrics) do
        table.insert(t, {
            apiName = metric.name or name,
            longName = metric.longName or metric[1] or name,
            description = metric.description or metric[2] or metric.longName,
            maker = metric.maker or metric[3].name or generate_maker_name(metric[3]),
            dependencyMetricNames = metric.dependencies or metric[4]
        })
    end
    return t
end

return M
