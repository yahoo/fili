// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.requestParameters;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of values for DataApiRequest building.
 */
public class DataApiRequestParametersImpl extends HashMap<String, Object> implements DataApiRequestParameters {

    public DataApiRequestParametersImpl(Map<String, Object> requestParameters) {
        super(requestParameters);
    }
}
