// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.beanimpl

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_GRANULARITY_MISMATCH

import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.time.Granularity
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.table.TableIdentifier
import com.yahoo.bard.webservice.web.ErrorMessageFormat
import com.yahoo.bard.webservice.web.apirequest.ApiRequest
import com.yahoo.bard.webservice.web.apirequest.ApiRequestImplSpec
import com.yahoo.bard.webservice.web.apirequest.utils.TestingApiRequestProvider
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException

import spock.lang.Specification

class ApiRequestBeanImplSpec extends ApiRequestImplSpec {

    ApiRequest buildApiRequestImpl(String format, String filename, String async, String perPage, String page) {
        return new ApiRequestBeanImpl(format, filename, async, perPage, page) {}
    }

    def setup() {
        //apiRequestImpl = TestingApiRequestProvider.buildBeanImpl();
    }
}
