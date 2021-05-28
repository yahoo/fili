// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.requestParameters;

import org.apache.commons.collections4.ListValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.PathSegment;

/**
 * Methods for translating from Jersey elements into non jersey elements for query building.
 */
public class RequestUtils {

    public static RequestColumn toRequestColumn(PathSegment pathSegment) {
        return new RequestColumnImpl(
                pathSegment.getPath(),
                toCollectionsMultiValueMap(pathSegment.getMatrixParameters())
        );
    }
    public static ListValuedMap<String, String> toCollectionsMultiValueMap(MultivaluedMap<String, String> jerseyMap) {
        ArrayListValuedHashMap<String, String> params = new ArrayListValuedHashMap<>();

        for (String key: jerseyMap.keySet()) {
            params.putAll(key, jerseyMap.get(key));
        }
        return params;
    }
}
