// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.data.ResultSet
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.util.Pagination
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.DateTime

import spock.lang.Specification
import spock.lang.Unroll

class ResponseWriterSelectorSpec extends Specification {

    private static FiliResponseWriterSelector filiResponseWriterSelector = new FiliResponseWriterSelector(
            new CsvResponseWriter(MAPPERS),
            new JsonResponseWriter(MAPPERS),
            new JsonApiResponseWriter(MAPPERS)
    )
    private static final DebugWriter debugWriter = new DebugWriter();

    private static final ObjectMappersSuite MAPPERS = new ObjectMappersSuite()

    SystemConfig systemConfig = SystemConfigProvider.getInstance()

    Set<Column> columns
    ResponseData response
    DateTime dateTime = new DateTime(1000L * 60 * 60 * 24 * 365 * 45)
    DataApiRequest apiRequest = Mock(DataApiRequest)
    ResultSet resultSet
    ByteArrayOutputStream os = new ByteArrayOutputStream()
    Pagination pagination
    SimplifiedIntervalList volatileIntervals = new SimplifiedIntervalList();

    static class DebugWriter implements ResponseWriter {
        @Override
        void write(final ApiRequest request, final ResponseData responseData, final OutputStream os)
                throws IOException {

        }
    }

    @Unroll
    def "test selector selecting correct writer for #typeName"() {
        setup:
        apiRequest.getFormat() >> type
        ResponseWriter actualWriter = filiResponseWriterSelector.select(apiRequest).get()

        expect:
        writer.isInstance(actualWriter)

        where:
        typeName  | writer                | type
        "JSON"    | JsonResponseWriter    | ResponseFormatType.JSON
        "CSV"     | CsvResponseWriter     | ResponseFormatType.CSV
        "JsonApi" | JsonApiResponseWriter | ResponseFormatType.JSONAPI
    }


    def "test selector selecting correct writer for customized type"() {
        setup:
        apiRequest.getFormat() >> ResponseFormatType.DEBUG
        filiResponseWriterSelector.addWriter(ResponseFormatType.DEBUG, debugWriter)
        ResponseWriter actualWriter = filiResponseWriterSelector.select(apiRequest).get()

        expect:
        DebugWriter.isInstance(actualWriter)
    }
}
