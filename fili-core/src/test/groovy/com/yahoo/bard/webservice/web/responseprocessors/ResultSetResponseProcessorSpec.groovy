// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.DruidResponseParser
import com.yahoo.bard.webservice.data.ResultSet
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper
import com.yahoo.bard.webservice.druid.client.FailureCallback
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback
import com.yahoo.bard.webservice.druid.model.QueryType
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.logging.RequestLog
import com.yahoo.bard.webservice.table.Schema
import com.yahoo.bard.webservice.web.DataApiRequest
import com.yahoo.bard.webservice.web.ResponseFormatType

import com.fasterxml.jackson.databind.JsonNode

import org.joda.time.DateTimeZone

import spock.lang.Specification

import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.core.HttpHeaders
import javax.ws.rs.core.MultivaluedMap
import javax.ws.rs.core.PathSegment
import javax.ws.rs.core.UriInfo

class ResultSetResponseProcessorSpec extends Specification {
    private static final ObjectMappersSuite MAPPERS = new ObjectMappersSuite()

    GroupByQuery groupByQuery
    DataApiRequest apiRequest
    AsyncResponse asyncResponse
    DruidResponseParser druidResponseParser
    UriInfo uriInfo
    PathSegment pathSegment
    MultivaluedMap paramMap

    Dimension d1

    ResultSetMapper rsm1
    LogicalMetric lm1
    ResultSet rs1

    def setup() {
        groupByQuery = Mock(GroupByQuery)
        asyncResponse = Mock(AsyncResponse)
        apiRequest = Mock(DataApiRequest)
        druidResponseParser = Mock(DruidResponseParser)
        uriInfo = Mock(UriInfo)
        pathSegment = Mock(PathSegment)
        paramMap = Mock(MultivaluedMap)

        paramMap.getFirst("dateTime") >> "a/b"
        pathSegment.getPath() >> "theMockPath"
        uriInfo.getPathSegments() >> Collections.singletonList(pathSegment)
        uriInfo.getQueryParameters() >> paramMap

        rsm1 = Mock(ResultSetMapper)
        lm1 = Mock(LogicalMetric)
        lm1.getCalculation() >> rsm1
        Set<LogicalMetric> metricSet = [lm1] as Set
        apiRequest.getLogicalMetrics() >> metricSet
        apiRequest.getGranularity() >> DAY
        apiRequest.getFormat() >> ResponseFormatType.JSON
        apiRequest.getUriInfo() >> uriInfo

        List<Dimension> dimensions = new ArrayList<Dimension>()
        List<Aggregation> aggregations = new ArrayList<Dimension>()
        List<PostAggregation> postAggs = new ArrayList<Dimension>()
        d1 = Mock(Dimension)
        dimensions.add(d1)
        d1.getDruidName() >> "dimension1"
        Aggregation agg1 = Mock(Aggregation)
        aggregations.add(agg1)
        agg1.getName() >> "agg1"

        PostAggregation postAgg1 = Mock(PostAggregation)
        postAggs.add(postAgg1)
        postAgg1.getName() >> "postAgg1"

        QueryType queryType = QueryType.GROUP_BY
        groupByQuery.getQueryType() >> queryType
        groupByQuery.getDimensions() >> dimensions
        groupByQuery.getAggregations() >> aggregations
        groupByQuery.getPostAggregations() >> postAggs

        Dimension d = Mock(Dimension)
        dimensions.add(d)
        Aggregation agg = Mock(Aggregation)
        aggregations.add(agg)
        PostAggregation postAgg = Mock(PostAggregation)
        postAggs.add(postAgg)

        Schema schema = new Schema(DAY)
        MetricColumn.addNewMetricColumn(schema, "lm1")
        rs1 = Mock(ResultSet)
        rs1.getSchema() >> schema

    }

    def "Test constructor"() {
        setup:
        def resultSetResponseProcessor = new ResultSetResponseProcessor(
                apiRequest,
                asyncResponse,
                druidResponseParser,
                MAPPERS
        ) {
        }

        expect:
        resultSetResponseProcessor.asyncResponse == asyncResponse
        resultSetResponseProcessor.granularity == DAY
        resultSetResponseProcessor.druidResponseParser == druidResponseParser
    }

    def "Test buildResultSet"() {
        setup:
        def resultSetResponseProcessor = new ResultSetResponseProcessor(
                apiRequest,
                asyncResponse,
                druidResponseParser,
                MAPPERS
        )
        JsonNode jsonMock = Mock(JsonNode)
        Schema captureSchema = null
        JsonNode captureJson
        ResultSet rs = Mock(ResultSet)

        1 * druidResponseParser.parse(_, _, _) >> {
            JsonNode json, Schema schema, QueryType type -> captureSchema = schema; captureJson = json; rs
        }
        ResultSet actual

        when:
        actual = resultSetResponseProcessor.buildResultSet(jsonMock, groupByQuery, DateTimeZone.UTC)
        DimensionColumn dimCol = captureSchema.getColumn("dimension1")
        MetricColumn m1 = captureSchema.getColumn("agg1")
        MetricColumn m2 = captureSchema.getColumn("postAgg1")

        then:
        captureSchema.granularity == DAY
        dimCol.dimension == d1
        m1 != null
        m2 != null
        actual == rs

    }

    def "A CSV DataApiRequest builds a response with the CSV content type header set"() {
        setup: "A ResultSetResponseProcessor"
        // See then block for apiRequest.getFormat() mock override
        def resultSetResponseProcessor = new ResultSetResponseProcessor(
                apiRequest,
                asyncResponse,
                druidResponseParser,
                MAPPERS
        )

        when: "The Response is built"
        javax.ws.rs.core.Response actual = resultSetResponseProcessor.buildResponse(rs1)

        then: "The header is set correctly"
        actual.getHeaderString(HttpHeaders.CONTENT_TYPE) == "text/csv; charset=utf-8"
        actual.getHeaderString(HttpHeaders.CONTENT_DISPOSITION) == "attachment; filename=theMockPath_a_b.csv"

        and: "Mock override: A CSV-formatted request"
        apiRequest.getFormat() >> ResponseFormatType.CSV
    }

    def "Test createResponseBuilder"() {
        setup:
        def resultSetResponseProcessor = new ResultSetResponseProcessor(
                apiRequest,
                asyncResponse,
                druidResponseParser,
                MAPPERS
        )
        when:
        javax.ws.rs.core.Response.ResponseBuilder rb
        rb = resultSetResponseProcessor.createResponseBuilder(rs1)

        then:
        rb != null
    }

    def "Test buildResponse"() {
        setup:
        javax.ws.rs.core.Response.ResponseBuilder responseBuilder = Mock(javax.ws.rs.core.Response.ResponseBuilder)

        def resultSetResponseProcessor = new ResultSetResponseProcessor(
                apiRequest,
                asyncResponse,
                druidResponseParser,
                MAPPERS
        ) {
            protected javax.ws.rs.core.Response.ResponseBuilder createResponseBuilder(ResultSet rs) {
                return responseBuilder
            }
        }

        MultivaluedMap<String, Object> headers = resultSetResponseProcessor.getHeaders()
        headers.add("one", 1)
        headers.add("two", "2")
        javax.ws.rs.core.Response expected = Mock(javax.ws.rs.core.Response)
        1 * responseBuilder.build() >> expected
        1 * responseBuilder.header("one", 1)
        1 * responseBuilder.header("two", "2")

        when:
        javax.ws.rs.core.Response actual = resultSetResponseProcessor.buildResponse(rs1)

        then:
        actual == expected
    }

    def "Test processResponse"() {
        setup:
        JsonNode jsonMock = Mock(JsonNode)
        ResultSet rs2 = Mock(ResultSet)
        javax.ws.rs.core.Response response = Mock(javax.ws.rs.core.Response)
        1 * asyncResponse.resume(response)
        1 * jsonMock.clone() >> null
        2 * rs2.getSchema() >> null

        ResultSetResponseProcessor resultSetResponseProcessor = new ResultSetResponseProcessor(
                apiRequest,
                asyncResponse,
                druidResponseParser,
                MAPPERS
        ) {
            @Override
            public ResultSet buildResultSet(JsonNode json, DruidAggregationQuery<?> groupByQuery, DateTimeZone dateTimeZone) {
                json.clone();
                return rs2
            }

            @Override
            protected javax.ws.rs.core.Response buildResponse(ResultSet resultSet) {
                resultSet.getSchema();
                return response
            }

            @Override
            protected ResultSet mapResultSet(ResultSet resultSet) { resultSet.getSchema(); return resultSet }
        }

        when:
        resultSetResponseProcessor.processResponse(
                jsonMock,
                groupByQuery,
                new ResponseContext(RequestLog.dump(), apiRequest)
        )

        then:
        1 == 1
    }

    def "Test failure callback"() {
        setup:
        def resultSetResponseProcessor = new ResultSetResponseProcessor(
                apiRequest,
                asyncResponse,
                druidResponseParser,
                MAPPERS
        )
        FailureCallback fbc = resultSetResponseProcessor.getFailureCallback()
        Throwable t = new Throwable("message1234")
        javax.ws.rs.core.Response responseCaptor = null
        1 * asyncResponse.resume(_) >> { javax.ws.rs.core.Response r -> responseCaptor = r }
        when:
        fbc.invoke(t)
        String entity = responseCaptor.entity
        then:
        responseCaptor.getStatus() == 500
        entity.contains("message1234")
    }


    def "Test error callback"() {
        setup:
        def resultSetResponseProcessor = new ResultSetResponseProcessor(
                apiRequest,
                asyncResponse,
                druidResponseParser,
                MAPPERS
        )
        HttpErrorCallback ec = resultSetResponseProcessor.getErrorCallback()
        javax.ws.rs.core.Response responseCaptor = null
        1 * asyncResponse.resume(_) >> { javax.ws.rs.core.Response r -> responseCaptor = r }
        when:
        ec.invoke(499, "myreason", "body123")
        String entity = responseCaptor.entity
        then:
        responseCaptor.getStatus() == 499
        entity.contains("myreason")
        entity.contains("body123")
    }
}
