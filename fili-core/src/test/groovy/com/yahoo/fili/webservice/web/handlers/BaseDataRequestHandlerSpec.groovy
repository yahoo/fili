// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.web.handlers

import com.yahoo.fili.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.fili.webservice.web.DataApiRequest
import com.yahoo.fili.webservice.web.responseprocessors.ResponseProcessor

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter

import spock.lang.Specification

class BaseDataRequestHandlerSpec extends Specification {

    def "Test base constructor"() {
        setup:
        ObjectMapper mapper = Mock(ObjectMapper)
        ObjectWriter writer = Mock(ObjectWriter)
        mapper.writer() >> writer
        BaseDataRequestHandler handler = new BaseDataRequestHandler(mapper) {
            public boolean handleRequest(
                RequestContext context,
                final DataApiRequest request,
                final DruidAggregationQuery<?> groupByQuery,
                final ResponseProcessor response
            ) {
                return true;
            }
        }

        expect:
        handler.mapper == mapper
        handler.writer == writer
    }
}
