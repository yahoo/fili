// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.data.DruidResponseParser;
import com.yahoo.bard.webservice.data.HttpResponseMaker;
import com.yahoo.bard.webservice.web.PreResponse;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;

import rx.subjects.Subject;

/**
 * A {@link ResponseProcessor} relies on things that are directly constructed at request time (i.e. the
 * {@link DataApiRequest}). Therefore, we can't inject a `ResponseProcessor` directly. We can however inject a factory.
 */
public interface ResponseProcessorFactory {

    /**
     * Constructs a custom ResponseProcessor.
     *
     * @param apiRequest  The current request
     * @param responseEmitter  Generates the response to be processed
     * @param druidResponseParser  Transforms a druid response into a {@link com.yahoo.bard.webservice.data.ResultSet}
     * @param objectMappers  Dictates how to format
     * @param httpResponseMaker  Crafts an HTTP response to be sent back to the user from a ResultSet or error message
     *
     * @return An object that handles parsing and post-processing of Druid requests
     */
    ResponseProcessor build(
            DataApiRequest apiRequest,
            Subject<PreResponse, PreResponse> responseEmitter,
            DruidResponseParser druidResponseParser,
            ObjectMappersSuite objectMappers,
            HttpResponseMaker httpResponseMaker
    );
}
