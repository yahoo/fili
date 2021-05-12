// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.requestParameters;

import org.apache.commons.collections4.ListValuedMap;

/**
 * A class modelling request columns.
 */
public class RequestColumnImpl implements RequestColumn {

    String apiName;
    ListValuedMap<String, String> values;

    /**
     * Constructor.
     *
     * @param apiName  the api name for the column
     * @param values  the value map for the column
     */
    public RequestColumnImpl(
            final String apiName,
            final ListValuedMap<String, String> values
    ) {
        this.apiName = apiName;
        this.values = values;
    }

    @Override
    public String getApiName() {
        return apiName;
    }

    @Override
    public ListValuedMap<String, String> getParameters() {
        return values;
    }
}
