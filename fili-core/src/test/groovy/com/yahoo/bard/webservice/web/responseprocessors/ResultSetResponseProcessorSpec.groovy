// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.druid.model.DefaultQueryType.GROUP_BY

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.DruidResponseParser
import com.yahoo.bard.webservice.data.HttpResponseChannel
import com.yahoo.bard.webservice.data.HttpResponseMaker
import com.yahoo.bard.webservice.data.ResultSet
import com.yahoo.bard.webservice.data.ResultSetSchema
import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper
import com.yahoo.bard.webservice.data.time.Granularity
import com.yahoo.bard.webservice.druid.client.FailureCallback
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback
import com.yahoo.bard.webservice.druid.model.DefaultQueryType
import com.yahoo.bard.webservice.druid.model.QueryType
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.logging.RequestLog
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.Schema
import com.yahoo.bard.webservice.table.TableTestUtils
import com.yahoo.bard.webservice.web.DefaultResponseFormatType
import com.yahoo.bard.webservice.web.JsonResponseWriter
import com.yahoo.bard.webservice.web.ResponseWriter
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.Sets

import org.joda.time.DateTimeZone

import rx.subjects.PublishSubject
import rx.subjects.Subject
import spock.lang.Specification

import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.MultivaluedMap
import javax.ws.rs.core.PathSegment
import javax.ws.rs.core.Response
import javax.ws.rs.core.UriInfo

class ResultSetResponseProcessorSpec extends Specification {

    private static final ObjectMappersSuite MAPPERS = new ObjectMappersSuite()

    private static final ObjectMapper MAPPER = MAPPERS.getMapper()

    HttpResponseMaker httpResponseMaker
    ResponseWriter responseWriter
    GroupByQuery groupByQuery
    DataApiRequest apiRequest
    HttpResponseChannel httpResponseChannel
    Subject responseEmitter
    DruidResponseParser druidResponseParser
    ContainerRequestContext containerRequestContext
    UriInfo uriInfo
    PathSegment pathSegment
    MultivaluedMap paramMap

    String dimension1Name = "dimension1"
    String dimension2Name = "dimension2"
    Dimension d1
    Dimension d2
    DimensionColumn dim1Column
    DimensionColumn dim2Column

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
        containerRequestContext = Mock(ContainerRequestContext)
        uriInfo = Mock(UriInfo)
        pathSegment = Mock(PathSegment)
        paramMap = Mock(MultivaluedMap)
        responseWriter = new JsonResponseWriter(MAPPERS)

        httpResponseMaker =  new HttpResponseMaker(MAPPERS, Mock(DimensionDictionary), responseWriter)
        httpResponseChannel = new HttpResponseChannel(
                asyncResponse,
                apiRequest,
                containerRequestContext,
                httpResponseMaker
        );
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
        apiRequest.getFormat() >> DefaultResponseFormatType.JSON
        containerRequestContext.getUriInfo() >> uriInfo

        List<Dimension> dimensions = new ArrayList<Dimension>()
        List<Aggregation> aggregations = new ArrayList<Dimension>()
        List<PostAggregation> postAggs = new ArrayList<Dimension>()
        d1 = Mock(Dimension) {
            getApiName() >> dimension1Name
        }
        Dimension d2 = Mock(Dimension) {
            getApiName() >> "dimension2"
        }
        dimensions.add(d1)
        dimensions.add(d2)
        dim1Column = new DimensionColumn(d1)
        dim2Column = new DimensionColumn(d2)


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


        DefaultQueryType queryType = GROUP_BY
        groupByQuery.getQueryType() >> queryType
        groupByQuery.getDimensions() >> dimensions
        groupByQuery.getAggregations() >> aggregations
        groupByQuery.getPostAggregations() >> postAggs
        groupByQuery.getDataSource() >> new TableDataSource(
                TableTestUtils.buildTable(
                        "table_name",
                        DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                        [] as Set,
                        ["dimension1": "dimension1"],
                        Mock(DataSourceMetadataService) { getAvailableIntervalsByDataSource(_ as DataSourceName) >> [:]}
                )
        )

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
        DimensionColumn dimCol = Mock(DimensionColumn) {
            getName() >> dimension1Name
            getDimension() >> d1
        }
        MetricColumn m1 = Mock(MetricColumn) {
            getName() >> metric1Name
        }

        MetricColumn m2 = Mock(MetricColumn) {
            getName() >> metric2Name
        }

        1 * druidResponseParser.parse(_, _, _, _) >> {
            JsonNode json, Schema schema, DefaultQueryType type, DateTimeZone dateTimeZone ->
                captureSchema = schema
                captureJson = json
                rs
        }
        druidResponseParser.buildSchemaColumns(groupByQuery) >> {
            [dimCol, m1, m2].stream()
        }

        ResultSet actual

        when:
        actual = resultSetResponseProcessor.buildResultSet(jsonMock, groupByQuery, DateTimeZone.UTC)

        then:
        captureSchema.granularity == DAY
        dimCol.dimension == d1
        captureSchema.getColumn(metric1Name, MetricColumn.class).get() == m1
        captureSchema.getColumn(metric2Name, MetricColumn.class).get() == m2
        captureSchema.getColumn("fakeMetric", MetricColumn.class) == Optional.empty()
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
        1 * httpResponseChannel.asyncResponse.resume(_) >> { Response r -> responseCaptor = r }
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
        1 * httpResponseChannel.asyncResponse.resume(_) >> { Response r -> responseCaptor = r }
        responseCaptor.getStatus() == 499
        entity.contains("myreason")
        entity.contains("body123")
    }

    def "Build the schema from the query"() {
        setup:
        ResultSetResponseProcessor processor = new ResultSetResponseProcessor(
                apiRequest,
                responseEmitter,
                druidResponseParser,
                MAPPERS,
                httpResponseMaker
        )

        Granularity granularity = apiRequest.granularity
        DateTimeZone dateTimeZone = Mock(DateTimeZone)
        Dimension dim = Mock(Dimension) { getApiName() >> dimension1Name }
        Aggregation agg = Mock(Aggregation) { getName() >> metric1Name }
        PostAggregation postAgg = Mock(PostAggregation) { getName() >> metric2Name }
        DruidAggregationQuery<?> query = Mock(DruidAggregationQuery){
            getAggregations() >> [agg]
            getPostAggregations() >> [postAgg]
            getDimensions() >> [dim]
            getQueryType() >> GROUP_BY
        }
        druidResponseParser.parse(_, _, _, _) >> {
            JsonNode json, ResultSetSchema schema, QueryType type, DateTimeZone tz -> new ResultSet(schema, [])
        }
        druidResponseParser.buildSchemaColumns(query) >> {
            [new DimensionColumn(dim), new MetricColumn(metric1Name), new MetricColumn(metric2Name)].stream()
        }
        ResultSet actual = processor.buildResultSet(MAPPER.readTree("[]"), query, dateTimeZone)

        expect:
        actual.schema == new ResultSetSchema(
                granularity,
                [new DimensionColumn(dim), new MetricColumn(metric1Name), new MetricColumn(metric2Name)]
        )
    }
}
