// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.map;

import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;

/**
 * Utilities to aid in converting to Map based ApiRequests.
 */
public class MapRequestUtil {

    /**
     * Convert from old constructor parameters.
     *
     * @param format  A string describing a response format.
     * @param downloadFilename  The filename to coerce the response to.
     * @param asyncAfter The time to wait before switching to asynchronous processing.
     * @param perPage The number of results per page.
     * @param page The page number.
     *
     * @return a map describing the parameter values.
     *
     */
    public static Map<String, Object> constructorConverter(
            String format,
            String downloadFilename,
            String asyncAfter,
            @NotNull String perPage,
            @NotNull String page
    ) {
        HashMap<String, Object> params = new HashMap<>();
        params.put(ApiRequestMapImpl.FORMAT_KEY, format);
        params.put(ApiRequestMapImpl.FILENAME_KEY, downloadFilename);
        params.put(ApiRequestMapImpl.ASYNC_AFTER_KEY, asyncAfter);
        params.put(ApiRequestMapImpl.PER_PAGE, perPage);
        params.put(ApiRequestMapImpl.PAGE, page);
        return params;
    }
}
