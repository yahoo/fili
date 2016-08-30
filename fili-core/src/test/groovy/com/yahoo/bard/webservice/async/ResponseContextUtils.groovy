// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContext

/**
 * A class containing utility methods to help tests associated with responseContext.
 */
class ResponseContextUtils {

    /**
     * Method to create a ResponseContext object using dimensionFields and a given map. Considering
     * empty map as default value for dimensionFields param
     *
     * @param responseContextValues  To be converted as ResponseContext
     * @param dimensionToDimensionFieldsMap  Dimension fields. By default it is empty map [:]
     *
     * @return ResponseContext object
     */
    public static ResponseContext createResponseContext(
            Map<String, Object> responseContextValues,
            Map<Dimension, Set<DimensionField>> dimensionToDimensionFieldsMap =  [:]
    ) {
        ResponseContext responseContext = new ResponseContext(dimensionToDimensionFieldsMap)
        responseContext.putAll(responseContextValues)
        return responseContext
    }
}
