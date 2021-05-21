// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.mapimpl;

import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.apirequest.ApiRequest;
import com.yahoo.bard.webservice.web.apirequest.generator.DefaultAsyncAfterGenerator;
import com.yahoo.bard.webservice.web.apirequest.generator.DefaultPaginationGenerator;
import com.yahoo.bard.webservice.web.apirequest.generator.DefaultResponseFormatGenerator;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * API Request. Abstract class offering default implementations for the common components of API request objects.
 */
public class ApiRequestMapImpl extends BaseRequestMapImpl implements ApiRequest {

    public static final String FILENAME_KEY = "fileName";
    public static final String FORMAT_KEY = "format";
    public static final String ASYNC_AFTER_KEY = "asyncAfter";
    public static final String GRANULARITY_KEY = "asyncAfter";


    public static final String PER_PAGE = "perPage";
    public static final String PAGE = "page";
    public static final String PAGINATION_PARAMETERS = "paginationParameters";

    static {
        putDefaultBinders(FILENAME_KEY, Function.identity());
        Function<String, ResponseFormatType> formatBinder = DefaultResponseFormatGenerator::generateResponseFormat;

        putDefaultBinders(FORMAT_KEY, formatBinder);

        Function<String, Long> asynchAfterBinder = DefaultAsyncAfterGenerator::generateAsyncAfter;
        putDefaultBinders(ASYNC_AFTER_KEY, asynchAfterBinder);

        BiFunction<String, String, Optional<PaginationParameters>> paginationParametersFunction =
                DefaultPaginationGenerator::generatePaginationParameters;
        putDefaultBinders(PAGINATION_PARAMETERS, paginationParametersFunction);

        Map<String, String> defaultValues  = new HashMap<String, String>() {{
            put(
                    ASYNC_AFTER_KEY,
                    SYSTEM_CONFIG.getStringProperty(SYSTEM_CONFIG.getPackageVariableName("default_asyncAfter"))
            );
            put(PER_PAGE, "");
            put(PAGE, "");
        }};
        addDefaultDefaultParamValues(defaultValues);
    }

    /**
     * Constructor.
     *
     * Constructor used to build ApiRequest objects.
     *
     * @param requestParams The map describing the parameters.
     */
    public ApiRequestMapImpl(Map<String, String> requestParams) {
        super(requestParams, Collections.emptyMap());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Optional<String> getDownloadFilename() {
        return (Optional<String>) bindAndGetOptionalProperty(FILENAME_KEY);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ResponseFormatType getFormat() {
        return (ResponseFormatType) bindAndGetNonOptionalProperty(FORMAT_KEY);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Optional<PaginationParameters> getPaginationParameters() {
        String perPage = (String) requestParams.getOrDefault(PER_PAGE, DEFAULT_PARAMS.get(PER_PAGE));
        String page = Optional.ofNullable((String) requestParams.get(PAGE)).orElse("");

        BiFunction<String, String, Optional<PaginationParameters>> binder =
                (BiFunction<String, String, Optional<PaginationParameters>>) binders.get(PAGINATION_PARAMETERS);
        return (binder.apply(perPage, page));
    }

    @Override
    public Long getAsyncAfter() {
        return (Long) bindAndGetNonOptionalProperty(ASYNC_AFTER_KEY);
    }
}
