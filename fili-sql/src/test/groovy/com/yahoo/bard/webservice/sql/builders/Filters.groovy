// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.builders

import com.yahoo.bard.webservice.druid.model.filter.AndFilter
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.filter.NotFilter
import com.yahoo.bard.webservice.druid.model.filter.OrFilter
import com.yahoo.bard.webservice.druid.model.filter.SearchFilter
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter

class Filters {
    public static SearchFilter search(String dimension) {
        return search(dimension, "")
    }

    public static SearchFilter search(String dimension, String search) {
        return new SearchFilter(
                SimpleDruidQueryBuilder.getDimension(dimension),
                SearchFilter.QueryType.Contains,
                search
        )
    }

    public static SelectorFilter select(String dimension, String search) {
        return new SelectorFilter(
                SimpleDruidQueryBuilder.getDimension(dimension),
                search
        )
    }

    public static NotFilter not(Filter filter) {
        return new NotFilter(filter)
    }

    public static AndFilter and(Filter... filter) {
        return new AndFilter(filter as List<Filter>)
    }

    public static OrFilter or(Filter... filter) {
        return new OrFilter(filter as List<Filter>)
    }
}
