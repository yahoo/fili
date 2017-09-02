// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.serializers;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonStreamContext;

import java.util.Optional;
import java.util.function.Function;

/**
 * Utility functions for druid serializers.
 */
public class SerializerUtil {

    /**
     * JSON tree walk up to physical table to retrieve physical name for a dimension.
     *
     * @param value  the dimension to retrieve api name.
     * @param gen  the Json Generator to retrieve the tree to walk on.
     *
     * @return  an Optional String of physical name
     */
    public static Optional<String> findPhysicalName(Dimension value, JsonGenerator gen) {
        String apiName = value.getApiName();
        // Search for physical name
        return mapNearestDruidQuery(
                gen,
                druidQuery -> druidQuery.getDataSource().getPhysicalTable().getPhysicalColumnName(apiName)
        );
    }

    /**
     * JSON tree walk to determine if there is a nested query below the current json node or not.
     *
     * @param gen  the Json Generator to retrieve the tree to walk on.
     *
     * @return  a Boolean where true indicates there are more nested query below this node, false otherwise
     */
    public static Boolean hasInnerQuery(JsonGenerator gen) {
        return mapNearestDruidQuery(gen, druidQuery -> druidQuery.getInnerQuery().isPresent()).orElse(false);
    }

    /**
     * JSON tree walk to find the druid query context of the current context and apply handler to the DruidQuery,
     * finds the current context if current context is a druid query.
     *
     * @param gen  the Json Generator to retrieve the tree to walk on.
     * @param mapper  a function that takes an DruidQuery as an argument and return the final desired returned result.
     * @param <T>  Type of result from the mapper
     *
     * @return an Optional of the desired result T if DruidQuery is found, Optional.empty otherwise
     */
    public static <T> Optional<T> mapNearestDruidQuery(JsonGenerator gen, Function<DruidQuery, T> mapper) {
        JsonStreamContext context = gen.getOutputContext();

        while (context != null) {
            Object parent = context.getCurrentValue();
            if (parent instanceof DruidQuery) {
                return Optional.of(mapper.apply((DruidQuery) parent));
            }
            context = context.getParent();
        }
        return Optional.empty();
    }
}
