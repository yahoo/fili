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
 * Builds the default Druid response processor: {@link ResultSetResponseProcessor}.
 */
public class ResultSetResponseProcessorFactory implements ResponseProcessorFactory {

    @Override
    public ResponseProcessor build(
            DataApiRequest apiRequest,
            Subject<PreResponse, PreResponse> responseEmitter,
            DruidResponseParser druidResponseParser,
            ObjectMappersSuite objectMappers,
            HttpResponseMaker httpResponseMaker
    ) {
        return new ResultSetResponseProcessor(
                apiRequest,
                responseEmitter,
                druidResponseParser,
                objectMappers,
                httpResponseMaker
        );
    }
}
