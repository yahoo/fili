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
    public static Map<String, String> constructorConverter(
            String format,
            String downloadFilename,
            String asyncAfter,
            String perPage,
            String page
    ) {
        HashMap<String, String> params = new HashMap<>();
        params.computeIfAbsent(ApiRequestMapImpl.FORMAT_KEY, val -> format);
        params.computeIfAbsent(ApiRequestMapImpl.FILENAME_KEY, val -> downloadFilename);
        params.computeIfAbsent(ApiRequestMapImpl.ASYNC_AFTER_KEY, val -> asyncAfter);
        params.computeIfAbsent(ApiRequestMapImpl.PER_PAGE, val -> perPage);
        params.computeIfAbsent(ApiRequestMapImpl.PAGE, val -> page);
        return params;
    }
}
