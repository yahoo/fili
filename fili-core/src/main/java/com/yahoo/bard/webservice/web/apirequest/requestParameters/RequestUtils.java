// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.requestParameters;

import org.apache.commons.collections4.ListValuedMap;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.ws.rs.container.ContainerRequestContext;
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

    public static DataApiRequestParameters fromRequest(ContainerRequestContext requestContext) {
        List<PathSegment> pathSegmentList = requestContext.getUriInfo().getPathSegments(true);
        Map<String, Object> collectedValues = new HashMap<>();
        assert "data" == pathSegmentList.get(1).getPath();

        // tablename
        collectedValues.put(DataApiRequestParameters.TABLE, pathSegmentList.get(2).getPath());
        collectedValues.put(DataApiRequestParameters.GRAIN, pathSegmentList.get(3).getPath());
        List<RequestColumn> columns = pathSegmentList.subList(4, pathSegmentList.size()).stream()
                .map(RequestUtils::toRequestColumn)
                .collect(Collectors.toList());
        collectedValues.put(DataApiRequestParameters.REQUEST_COLUMNS, columns);
        MultiValuedMap<String, String> queryParameters =
                RequestUtils.toCollectionsMultiValueMap(requestContext.getUriInfo()
                        .getQueryParameters(true));

        for (String key : queryParameters.keySet()) {
            Collection<String> queryParamValues = queryParameters.get(key);
            assert queryParamValues.size() == 1;
            collectedValues.put(key, queryParamValues.stream().findFirst().orElse(""));
        }
        return new DataApiRequestParametersImpl(collectedValues);
    }
}
