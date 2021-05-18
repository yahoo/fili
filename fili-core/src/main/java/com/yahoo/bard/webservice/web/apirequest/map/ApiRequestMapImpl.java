// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.map;

import static com.yahoo.bard.webservice.web.apirequest.generator.DefaultResponseFormatGenerator.generateResponseFormat;

import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.apirequest.ApiRequest;
import com.yahoo.bard.webservice.web.apirequest.ApiRequestBeanImpl;
import com.yahoo.bard.webservice.web.apirequest.generator.DefaultAsyncAfterGenerator;
import com.yahoo.bard.webservice.web.apirequest.generator.DefaultPaginationGenerator;
import com.yahoo.bard.webservice.web.apirequest.generator.DefaultResponseFormatGenerator;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * API Request. Abstract class offering default implementations for the common components of API request objects.
 */
public class ApiRequestMapImpl implements ApiRequest {

    private static final Logger LOG = LoggerFactory.getLogger(ApiRequestBeanImpl.class);

    public static final String FILENAME_KEY = "fileName";
    public static final String FORMAT_KEY = "format";
    public static final String ASYNC_AFTER_KEY = "asyncAfter";

    public static final String PER_PAGE = "perPage";
    public static final String PAGE = "page";
    public static final String PAGINATION_PARAMETERS = "paginationParameters";

//            return DefaultPaginationGenerator.generatePaginationParameters(perPage, page);

    public static final Map<String, Object> DEFAULT_BINDERS = new HashMap<>();

    static {
        DEFAULT_BINDERS.put(FILENAME_KEY, Function.identity());
        Function<String, ResponseFormatType> formatBinder = DefaultResponseFormatGenerator::generateResponseFormat;
        DEFAULT_BINDERS.put(FORMAT_KEY, formatBinder);
        Function<String, Long> asynchAfterBinder = DefaultAsyncAfterGenerator::generateAsyncAfter;
        DEFAULT_BINDERS.put(ASYNC_AFTER_KEY, formatBinder);
        BiFunction<String, String, Optional<PaginationParameters>> paginationParametersFunction =
                DefaultPaginationGenerator::generatePaginationParameters;
        DEFAULT_BINDERS.put(PAGINATION_PARAMETERS, paginationParametersFunction);
    }

    private final Map<String, Object> requestParams;
    private final Map<String, Object> binders;

    /**
     * Constructor.
     *
     * Constructor used to build ApiRequest objects.
     *
     * @param prototype The map describing the parameters.
     */
    public ApiRequestMapImpl(Map<String, Object> prototype) {
        requestParams = new HashMap<>();
        requestParams.putAll(prototype);
        binders = new HashMap<>(DEFAULT_BINDERS);
    }

    @SuppressWarnings("unchecked")
    public Optional<?> getOptionalBoundProperty(String key) {
        Optional<String> value = Optional.ofNullable((String) requestParams.get(key));
        Function<String, ?> binder = (Function<String, ?>) binders.get(key);
        return value.map(binder);
    }

    @SuppressWarnings("unchecked")
    public Object getNonOptionalBoundProperty(String key) {
        Optional<String> value = Optional.ofNullable((String) requestParams.get(key));
        Function<String, ?> binder = (Function<String, ?>) binders.get(key);
        return (value.map(binder).orElse(null));
    }

    public void setRequestParameter(String key, String value) {
        requestParams.put(key, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Optional<String> getDownloadFilename() {
        return (Optional<String>) getOptionalBoundProperty(FILENAME_KEY);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ResponseFormatType getFormat() {
        return (ResponseFormatType) getNonOptionalBoundProperty(FORMAT_KEY);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Optional<PaginationParameters> getPaginationParameters() {
        String perPage = Optional.ofNullable((String) requestParams.get(PER_PAGE)).orElse("");
        String page = Optional.ofNullable((String) requestParams.get(PAGE)).orElse("");

        BiFunction<String, String, Optional<PaginationParameters>> binder =
                (BiFunction<String, String, Optional<PaginationParameters>>) binders.get(PAGINATION_PARAMETERS);
        return (binder.apply(perPage, page));
    }

    @Override
    public Long getAsyncAfter() {
        return (Long) getNonOptionalBoundProperty(ASYNC_AFTER_KEY);
    }
}
