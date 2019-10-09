// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.presto;


import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.sql.ApiToFieldMapper;
import com.yahoo.bard.webservice.sql.SqlBackedClient;
import com.yahoo.bard.webservice.sql.helper.CalciteHelper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import javax.sql.DataSource;

/**
 * Converts druid queries to sql, convert it to presto dialect, executes it, and returns a druid like response.
 */
public class PrestoSqlBackedClient implements SqlBackedClient {
    private static final Logger LOG = LoggerFactory.getLogger(PrestoSqlBackedClient.class);
    private final ObjectMapper jsonWriter;
    private final DruidQueryToPrestoConverter druidQueryToPrestoConverter;
    private final CalciteHelper calciteHelper;

    /**
     * Creates a sql converter using the given database and datasource.
     *
     * @param dataSource The presto datasource used for connection.
     * @param objectMapper  The mapper for all JSON processing.
     *
     * @throws SQLException if can't read from database.
     */
    public PrestoSqlBackedClient(DataSource dataSource, ObjectMapper objectMapper) throws SQLException {
        calciteHelper = new CalciteHelper(dataSource);
        druidQueryToPrestoConverter = new DruidQueryToPrestoConverter(calciteHelper);
        jsonWriter = objectMapper;
    }

    @Override
    public Future<JsonNode> executeQuery(
            DruidQuery<?> druidQuery,
            SuccessCallback successCallback,
            FailureCallback failureCallback
    ) {

        RequestLog logCtx = RequestLog.dump();
        //todo eventually stop/start RequestLog phases
        return CompletableFuture.supplyAsync(() -> {
                    try {
                        JsonNode jsonNode = executeAndProcessQuery((DruidAggregationQuery) druidQuery);
                        if (successCallback != null) {
                            successCallback.invoke(jsonNode);
                        }
                        return jsonNode;
                    } catch (Throwable t) {
                        LOG.warn("Failed while querying ", t);
                        if (failureCallback != null) {
                            failureCallback.dispatch(t);
                        }
                    } finally {
                        RequestLog.restore(logCtx);
                    }
                    return null;
                }
        );
    }

    /**
     * Builds sql for a druid query, convert it to presto dialect,
     * execute it against the database, process the results
     * and return a jsonNode in the format of a druid response.
     *
     * @param druidQuery  The druid query to build and process.
     *
     * @return a druid-like response to the query.
     */
    private JsonNode executeAndProcessQuery(DruidAggregationQuery<?> druidQuery) {
        if (!druidQueryToPrestoConverter.isValidQuery(druidQuery)) {
            throw new UnsupportedOperationException("Unable to process " + druidQuery);
        }

        ApiToFieldMapper aliasMaker =
                new ApiToFieldMapper(druidQuery.getDataSource().getPhysicalTable().getSchema());
        String sqlQuery = druidQueryToPrestoConverter.buildSqlQuery(druidQuery, aliasMaker);

        LOG.info("sqlQuery: {}", sqlQuery);
        sqlQuery = sqlQueryToPrestoQuery(sqlQuery);

        PrestoResultSetProcessor resultSetProcessor = new PrestoResultSetProcessor(
                druidQuery,
                aliasMaker,
                jsonWriter,
                druidQueryToPrestoConverter.getTimeConverter()
        );

        LOG.info("getConnection in PrestoSqlBackedClient");
        try (Connection connection = calciteHelper.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sqlQuery);
            resultSetProcessor.process(resultSet);
            JsonNode jsonNode = resultSetProcessor.buildDruidResponse();
            return jsonNode;
        } catch (SQLException e) {
            LOG.warn("Failed while processing {}", druidQuery);
            throw new RuntimeException("Couldn't generate sql", e);
        }
    }

    /**
     * Converts sql dialect into presto compatible dialect.
     * TODO: Add functionality to specify catalog and schema dynamically.
     * Steps are:
     * <ul>
     *     <li>1. Replace Time functions with String Functions to be used for HIVE tables</li>
     *     <li>2. Convert datestamp from Date format to String format</li>
     *     <Li>3. Specify Catalog in FROM expression</Li>
     *     <li>4. Calcite wrap column name in single quotes which is not allowed in Presto
     *            as Presto is not ANSI compliant.
     *            Replace single quotation marks with double quotation marks.</li>
     *     <li>5. Convert FETCH NEXT n ROWS expression to prestodb-supported LIMIT n expression</li>
     * </ul>
     *
     * @param sqlQuery The sql query converted from druid query.
     *
     * @return a presto compatible sql dialect.
     * */
    private static String sqlQueryToPrestoQuery(String sqlQuery) {

        String fixTimePrestoQuery = sqlQuery
                .replace("DAYOFYEAR(\"datestamp\")", "DAY_OF_YEAR(date_parse(SUBSTRING (datestamp,1,10),\'%Y%m%d%H\'))")
                .replace(" YEAR(\"datestamp\")", " SUBSTRING(datestamp,1,4)")
                .replace("MONTH(\"datestamp\")", "SUBSTRING(datestamp,5,2)")
                .replace("HOUR(\"datestamp\")", "SUBSTRING(datestamp,9,2)");

        int datestampStartPosition = fixTimePrestoQuery.indexOf("\"datestamp\" >");
        int datestampNumericStartPosition = fixTimePrestoQuery.indexOf('\'', datestampStartPosition);
        fixTimePrestoQuery = fixTimePrestoQuery.substring(0, datestampNumericStartPosition + 5) +
                fixTimePrestoQuery.substring(datestampNumericStartPosition + 6, datestampNumericStartPosition + 8) +
                fixTimePrestoQuery.substring(datestampNumericStartPosition + 9, datestampNumericStartPosition + 11) +
                fixTimePrestoQuery.substring(datestampNumericStartPosition + 12, datestampNumericStartPosition + 14) +
                fixTimePrestoQuery.substring(datestampNumericStartPosition + 15, datestampNumericStartPosition + 17) +
                fixTimePrestoQuery.substring(datestampNumericStartPosition + 18, datestampNumericStartPosition + 20) +
                fixTimePrestoQuery.substring(datestampNumericStartPosition + 21);

        datestampStartPosition = fixTimePrestoQuery.indexOf("\"datestamp\" <");
        datestampNumericStartPosition = fixTimePrestoQuery.indexOf('\'', datestampStartPosition);
        fixTimePrestoQuery = fixTimePrestoQuery.substring(0, datestampNumericStartPosition + 5) +
                fixTimePrestoQuery.substring(datestampNumericStartPosition + 6, datestampNumericStartPosition + 8) +
                fixTimePrestoQuery.substring(datestampNumericStartPosition + 9, datestampNumericStartPosition + 11) +
                fixTimePrestoQuery.substring(datestampNumericStartPosition + 12, datestampNumericStartPosition + 14) +
                fixTimePrestoQuery.substring(datestampNumericStartPosition + 15, datestampNumericStartPosition + 17) +
                fixTimePrestoQuery.substring(datestampNumericStartPosition + 18, datestampNumericStartPosition + 20) +
                fixTimePrestoQuery.substring(datestampNumericStartPosition + 21);

//        String fixCatalogPrestoQuery = fixTimePrestoQuery
//                .replace("\"spotlight_hive\"", "\"dilithiumblue\".\"spotlight_hive\"");

        String fixCatalogPrestoQuery = fixTimePrestoQuery;

        int orderbyIndex = fixCatalogPrestoQuery.indexOf("ORDER BY");
        final int orderByClauseLength = 9;
        String fixQuotePrestoQuery = fixCatalogPrestoQuery;
        if (orderbyIndex != -1) {
            orderbyIndex = orderbyIndex + orderByClauseLength;
            //this way other part won't break abnormally.
            String orderByClause = fixCatalogPrestoQuery.substring(orderbyIndex);
            orderByClause = orderByClause
                    .replace("'", "\"")   //replace double quotes into single quotes
                    .replace("\"%", "'%") //time stamp needs single quotes
                    .replace("H\"", "H'");
            fixQuotePrestoQuery = fixCatalogPrestoQuery.substring(0, orderbyIndex) + orderByClause;
        }

        String limitPrestoQuery = fetchToLimitHelper(fixQuotePrestoQuery);
        LOG.info("In sqlQueryToPrestoQuery processed sqlQuery: {}", limitPrestoQuery);
        return limitPrestoQuery;
    }

    /**
     * Helper function that converts FETCH NEXT into LIMIT expression
     * as Calcite converts limitspecs to FETCH NEXT
     * but prestodb does not have support for FETCH NEXT.
     *
     * @param fixQuotePrestoQuery The intermediate sqlToPrestoQuery.
     *
     * @return a presto query with LIMIT fix.
     * */
    private static String fetchToLimitHelper(String fixQuotePrestoQuery) {
        String limitPrestoQuery = fixQuotePrestoQuery;
        int index = limitPrestoQuery.indexOf("FETCH NEXT");
        if (index != -1) {
            String prevPart = limitPrestoQuery.substring(0, index);
            //parsing numbers
            String nextPart = limitPrestoQuery.substring(index);
            int rows = 0;
            boolean numFlag = false;
            for (int i = 0; i < nextPart.length(); i++) {
                //not a number
                if (nextPart.charAt(i) > '9' || nextPart.charAt(i) < '0') {
                    if (!numFlag) {
                        continue;
                    } else {
                        break;
                    }
                } else {
                    numFlag = true;
                    rows *= 10;
                    rows += (nextPart.charAt(i) - '0');
                }
            }
            int idxOfLineReturn = nextPart.indexOf('\n');
            if (idxOfLineReturn != -1) {
                nextPart = "LIMIT " + rows + nextPart.substring(idxOfLineReturn);

            } else {
                nextPart = "LIMIT " + rows;
            }
            limitPrestoQuery = prevPart + nextPart;
        }
        return limitPrestoQuery;
    }
}
