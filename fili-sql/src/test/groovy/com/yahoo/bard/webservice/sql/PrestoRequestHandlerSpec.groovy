// Copyright 2019 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
import com.yahoo.bard.webservice.druid.client.SuccessCallback
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.sql.SqlAggregationQuery
import com.yahoo.bard.webservice.sql.presto.PrestoSqlBackedClient
import com.yahoo.bard.webservice.table.ConstrainedTable
import com.yahoo.bard.webservice.table.SqlPhysicalTable
import com.yahoo.bard.webservice.table.StrictPhysicalTable
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest
import com.yahoo.bard.webservice.web.handlers.DataRequestHandler
import com.yahoo.bard.webservice.web.handlers.PrestoRequestHandler
import com.yahoo.bard.webservice.web.handlers.RequestContext
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor

import com.fasterxml.jackson.databind.JsonNode

import spock.lang.Specification

import java.util.concurrent.Future

class PrestoRequestHandlerSpec extends Specification {
    def "Test handle Presto request handler to presto route"() {
        setup:
        PrestoSqlBackedClient prestoSqlBackedClient = Mock(PrestoSqlBackedClient)
        DataRequestHandler dataRequestHandler = Mock(DataRequestHandler)


        RequestContext requestContext = Mock(RequestContext)
        DataApiRequest dataApiRequest = Mock(DataApiRequest)

        SqlPhysicalTable sqlPhysicalTable = Mock(SqlPhysicalTable)
        ConstrainedTable constrainedTable = Mock(ConstrainedTable) {
            getSourceTable() >> sqlPhysicalTable
        }
        TableDataSource tableDataSource = Mock(TableDataSource) {
            getPhysicalTable() >> constrainedTable
        }
        DruidAggregationQuery druidQuery = Mock(DruidAggregationQuery) {
            getDataSource() >> tableDataSource
        }
        druidQuery.getIntervals() >> Collections.emptyList()
        ResponseProcessor responseProcessor = Mock(ResponseProcessor)


        JsonNode jsonNode = Mock(JsonNode)

        PrestoRequestHandler handler = new PrestoRequestHandler(dataRequestHandler, prestoSqlBackedClient)

        SuccessCallback sc = null

        boolean success

        when:
        success = handler.handleRequest(requestContext, dataApiRequest, druidQuery, responseProcessor)

        then:
        success
        1 * responseProcessor.getFailureCallback(druidQuery)
        1 * prestoSqlBackedClient.executeQuery(druidQuery, _, _) >> { a0, a1, a2 ->
            sc = a1
            return Mock(Future)
        }
        when:
        sc.invoke(jsonNode)
        then:
        1 * responseProcessor.processResponse(jsonNode, _, _) >> {
            _, SqlAggregationQuery sqlAggregationQuery, loggingContext ->
                assert sqlAggregationQuery.getDataSource() == tableDataSource
                assert loggingContext != null
        }
    }

    def "Test handle Presto request handler to next handler route when source table is not SqlPhysicalTable"() {
        setup:
        PrestoSqlBackedClient prestoSqlBackedClient = Mock(PrestoSqlBackedClient)
        DataRequestHandler dataRequestHandler = Mock(DataRequestHandler)
        StrictPhysicalTable sqlPhysicalTable = Mock(StrictPhysicalTable)
        ConstrainedTable constrainedTable = Mock(ConstrainedTable) {
            getSourceTable() >> sqlPhysicalTable
        }
        TableDataSource tableDataSource = Mock(TableDataSource) {
            getPhysicalTable() >> constrainedTable
        }
        DruidAggregationQuery druidQuery = Mock(DruidAggregationQuery) {
            getDataSource() >> tableDataSource
        }
        ResponseProcessor responseProcessor = Mock(ResponseProcessor)
        RequestContext requestContext = Mock(RequestContext)
        DataApiRequest dataApiRequest = Mock(DataApiRequest)
        PrestoRequestHandler handler = new PrestoRequestHandler(dataRequestHandler, prestoSqlBackedClient)

        when:
        handler.handleRequest(requestContext, dataApiRequest, druidQuery, responseProcessor)

        then:
        1 * dataRequestHandler.handleRequest(requestContext, dataApiRequest, druidQuery, responseProcessor)
    }
}
