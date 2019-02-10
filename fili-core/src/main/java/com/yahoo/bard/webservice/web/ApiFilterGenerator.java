// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.web.apirequest.binders.FilterBinders;

import javax.validation.constraints.NotNull;

/**
 * Factory for ApiFilter objects. Has methods to build an ApiFilter object out of the filter clause in a Fili Api
 * request, and producing an ApiFilter by unioning the values of two provided ApiFilters.
 *
 * @deprecated Methods merged with {@link FilterBinders}
 */
@Deprecated
public class ApiFilterGenerator {
    /**
     * Parses the URL filter Query and generates the ApiFilter object.
     *
     * @param filterQuery  Expects a URL filter query String in the format:
     * <code>(dimension name)|(field name)-(operation)[?(value or comma separated values)]?</code>
     * @param dimensionDictionary  cache containing all the valid dimension objects.
     *
     * @return the ApiFilter
     * @throws BadFilterException Exception when filter pattern is not matched or when any of its properties are not
     * valid.
     *
     * @deprecated Use {@link FilterBinders#generateApiFilter(String, DimensionDictionary)}
     */
    @Deprecated
    public static ApiFilter build(
            @NotNull String filterQuery,
            DimensionDictionary dimensionDictionary
    ) throws BadFilterException {
        return FilterBinders.INSTANCE.generateApiFilter(filterQuery, dimensionDictionary);
    }

    /**
     * Take two Api filters which differ only by value sets and union their value sets.
     *
     * @param one  The first ApiFilter
     * @param two  The second ApiFilter
     *
     * @return an ApiFilter with the union of values
     *
     * @deprecated Use {@link FilterBinders#union(ApiFilter, ApiFilter)}
     */
    @Deprecated
    public static ApiFilter union(ApiFilter one, ApiFilter two) {
        return FilterBinders.INSTANCE.union(one, two);
    }
}
