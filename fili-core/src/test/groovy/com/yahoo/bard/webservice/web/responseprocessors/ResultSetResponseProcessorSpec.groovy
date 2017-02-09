// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.DruidResponseParser
import com.yahoo.bard.webservice.data.HttpResponseChannel
import com.yahoo.bard.webservice.data.HttpResponseMaker
import com.yahoo.bard.webservice.data.ResultSet
import com.yahoo.bard.webservice.data.ResultSetSchema
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper
import com.yahoo.bard.webservice.druid.client.FailureCallback
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback
import com.yahoo.bard.webservice.druid.model.DefaultQueryType
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.logging.RequestLog
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.ConcretePhysicalTable
import com.yahoo.bard.webservice.table.Schema
import com.yahoo.bard.webservice.web.DataApiRequest
import com.yahoo.bard.webservice.web.ResponseFormatType

import com.fasterxml.jackson.databind.JsonNode

import org.joda.time.DateTimeZone

import avro.shaded.com.google.common.collect.Sets
import rx.subjects.PublishSubject
import rx.subjects.Subject
import spock.lang.Specification

import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.core.MultivaluedMap
import javax.ws.rs.core.PathSegment
import javax.ws.rs.core.Response
import javax.ws.rs.core.UriInfo

class ResultSetResponseProcessorSpec extends Specification {
    private static final ObjectMappersSuite MAPPERS = new ObjectMappersSuite()

    HttpResponseMaker httpResponseMaker
    GroupByQuery groupByQuery
    DataApiRequest apiRequest
    HttpResponseChannel httpResponseChannel
    Subject responseEmitter
    DruidResponseParser druidResponseParser
    UriInfo uriInfo
    PathSegment pathSegment
    MultivaluedMap paramMap

    String dimension1Name = "dimension1"
    Dimension d1
    Dimension d2

    String metric1Name = "agg1"
    String metric2Name = "postagg1"

    ResultSetMapper rsm1
    LogicalMetric lm1
    ResultSet rs1

    def setup() {
        groupByQuery = Mock(GroupByQuery)
        AsyncResponse asyncResponse = Mock(AsyncResponse)
        apiRequest = Mock(DataApiRequest)
        druidResponseParser = Mock(DruidResponseParser)
        uriInfo = Mock(UriInfo)
        pathSegment = Mock(PathSegment)
        paramMap = Mock(MultivaluedMap)

        httpResponseMaker =  new HttpResponseMaker(MAPPERS, Mock(DimensionDictionary))
        httpResponseChannel = new HttpResponseChannel(asyncResponse, apiRequest, httpResponseMaker);
        responseEmitter = PublishSubject.create()
        responseEmitter.subscribe(httpResponseChannel)

        paramMap.getFirst("dateTime") >> "a/b"
        pathSegment.getPath() >> "theMockPath"
        uriInfo.getPathSegments() >> Collections.singletonList(pathSegment)
        uriInfo.getQueryParameters() >> paramMap

        rsm1 = Mock(ResultSetMapper)
        lm1 = Mock(LogicalMetric)
        lm1.getCalculation() >> rsm1
        lm1.getName() >> "metric1"
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
        d1.getApiName() >> dimension1Name
        Dimension d2 = Mock(Dimension)
        dimensions.add(d2)
        d2.getApiName() >> "dimension2"

        Aggregation agg1 = Mock(Aggregation)
        aggregations.add(agg1)
        agg1.getName() >> metric1Name
        Aggregation agg2 = Mock(Aggregation)
        agg2.getName() >> "otherAgg"
        aggregations.add(agg2)

        PostAggregation postAgg1 = Mock(PostAggregation)
        postAggs.add(postAgg1)
        postAgg1.getName() >> metric2Name
        PostAggregation postAgg2 = Mock(PostAggregation)
        postAggs.add(postAgg2)
        postAgg2.getName() >> "otherPostAgg"


        DefaultQueryType queryType = DefaultQueryType.GROUP_BY
        groupByQuery.getQueryType() >> queryType
        groupByQuery.getDimensions() >> dimensions
        groupByQuery.getAggregations() >> aggregations
        groupByQuery.getPostAggregations() >> postAggs
        groupByQuery.getDataSource() >> new TableDataSource(new ConcretePhysicalTable("table_name", DAY.buildZonedTimeGrain(DateTimeZone.UTC), [] as Set, ["dimension1":"dimension1"], Mock(DataSourceMetadataService)))



        ResultSetSchema schema = new ResultSetSchema(DAY, Sets.newHashSet(new MetricColumn("lm1")))

        rs1 = Mock(ResultSet)
        rs1.getSchema() >> schema

        apiRequest.getDimensionFields() >> [
                (d1): [BardDimensionField.ID] as Set
        ]
    }

    def "Test constructor"() {
        setup:
        def resultSetResponseProcessor = new ResultSetResponseProcessor(
                apiRequest,
                responseEmitter,
                druidResponseParser,
                MAPPERS,
                httpResponseMaker
        ) {
        }

        expect:
        resultSetResponseProcessor.responseEmitter == responseEmitter
        resultSetResponseProcessor.granularity == DAY
        resultSetResponseProcessor.druidResponseParser == druidResponseParser
    }

    def "Test buildResultSet"() {
        setup:
        def resultSetResponseProcessor = new ResultSetResponseProcessor(
                apiRequest,
                responseEmitter,
                druidResponseParser,
                MAPPERS,
                httpResponseMaker
        )
        JsonNode jsonMock = Mock(JsonNode)
        ResultSetSchema captureSchema = null
        JsonNode captureJson
        ResultSet rs = Mock(ResultSet)

        1 * druidResponseParser.parse(_, _, _, _) >> {
            JsonNode json, Schema schema, DefaultQueryType type, DateTimeZone dateTimeZone
                ->
                captureSchema = schema;
                captureJson = json;
                rs
        }
        ResultSet actual

        when:
        actual = resultSetResponseProcessor.buildResultSet(jsonMock, groupByQuery, DateTimeZone.UTC)
        DimensionColumn dimCol = captureSchema.getColumn(dimension1Name, DimensionColumn.class).get()
        MetricColumn m1 = captureSchema.getColumn(metric1Name, MetricColumn.class).get()
        MetricColumn m2 = captureSchema.getColumn(metric2Name, MetricColumn.class).get()
        Optional<MetricColumn> m3 = captureSchema.getColumn("fakeMetric", MetricColumn.class)

        then:
        captureSchema.granularity == DAY
        dimCol.dimension == d1
        m1 != null
        m2 != null
        m3 == Optional.empty()
        actual == rs

    }

    def "Test processResponse"() {
        setup:
        JsonNode jsonMock = Mock(JsonNode)
        ResultSet resultSetMock = Mock(ResultSet)

        ResultSetResponseProcessor resultSetResponseProcessor = new ResultSetResponseProcessor(
                apiRequest,
                responseEmitter,
                druidResponseParser,
                MAPPERS,
                httpResponseMaker
        ) {
            @Override
            public ResultSet buildResultSet(
                    JsonNode json,
                    DruidAggregationQuery<?> groupByQuery,
                    DateTimeZone dateTimeZone
            ) {
                json.clone();
                return resultSetMock
            }

            @Override
            protected ResultSet mapResultSet(ResultSet resultSet) { resultSet.getSchema(); return resultSet }
        }

        when:
        resultSetResponseProcessor.processResponse(
                jsonMock,
                groupByQuery,
                new LoggingContext(RequestLog.dump())
        )

        then:
        1 * jsonMock.clone()
        2 * resultSetMock.getSchema()
        1 == 1
    }

    def "Test failure callback"() {
        setup:
        def resultSetResponseProcessor = new ResultSetResponseProcessor(
                apiRequest,
                responseEmitter,
                druidResponseParser,
                MAPPERS,
                httpResponseMaker
        )
        FailureCallback fbc = resultSetResponseProcessor.getFailureCallback()
        Throwable t = new Throwable("message1234")
        Response responseCaptor = null
        1 * httpResponseChannel.asyncResponse.resume(_) >> { javax.ws.rs.core.Response r -> responseCaptor = r }
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
                responseEmitter,
                druidResponseParser,
                MAPPERS,
                httpResponseMaker
        )
        HttpErrorCallback ec = resultSetResponseProcessor.getErrorCallback()
        Response responseCaptor = null

        when:
        ec.invoke(499, "myreason", "body123")
        String entity = responseCaptor.entity

        then:
        1 * httpResponseChannel.asyncResponse.resume(_) >> { javax.ws.rs.core.Response r -> responseCaptor = r }
        responseCaptor.getStatus() == 499
        entity.contains("myreason")
        entity.contains("body123")
    }
}
