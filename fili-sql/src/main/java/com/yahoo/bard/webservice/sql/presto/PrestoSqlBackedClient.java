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
import com.yahoo.bard.webservice.sql.SqlResultSetProcessor;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
     * @param dataSource   The presto datasource used for connection.
     * @param objectMapper The mapper for all JSON processing.
     */
    public PrestoSqlBackedClient(DataSource dataSource, ObjectMapper objectMapper) {
        try {
            calciteHelper = new CalciteHelper(dataSource);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to start PrestoSqlBackedClient.", e);
        }
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
     * @param druidQuery The druid query to build and process.
     * @return a druid-like response to the query.
     */
    private JsonNode executeAndProcessQuery(DruidAggregationQuery<?> druidQuery) {
        if (!druidQueryToPrestoConverter.isValidQuery(druidQuery)) {
            throw new UnsupportedOperationException("Unable to process " + druidQuery);
        }

        ApiToFieldMapper aliasMaker =
                new ApiToFieldMapper(druidQuery.getDataSource().getPhysicalTable().getSchema());
        String sqlQuery = druidQueryToPrestoConverter.buildSqlQuery(druidQuery, aliasMaker);

        LOG.info("Input raw sql query: {}", sqlQuery);
        sqlQuery = sqlQueryToPrestoQuery(sqlQuery);
        LOG.info("Processed to presto query: {}", sqlQuery);

        SqlResultSetProcessor resultSetProcessor = new SqlResultSetProcessor(
                druidQuery,
                aliasMaker,
                jsonWriter,
                druidQueryToPrestoConverter.getTimeConverter()
        );

        try (Connection connection = calciteHelper.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sqlQuery);
            resultSetProcessor.process(resultSet);
            JsonNode jsonNode = resultSetProcessor.buildDruidResponse();
            LOG.info("Created response: {}", jsonNode);
            return jsonNode;
        } catch (SQLException e) {
            LOG.warn("Failed while processing {}", druidQuery);
            throw new RuntimeException("Couldn't generate sql", e);
        }
    }

    /**
     * Converts sql dialect into presto compatible dialect. The input sqlQuery must follow the format of
     * the output of DruidQueryToPrestoConverter.buildSqlQuery()
     * Steps are:
     * <ul>
     *     <li>1. Replace Time functions with String Functions to be used for HIVE tables</li>
     *     <li>2. Calcite wrap column name in single quotes which is not allowed in Presto
     *            as Presto is not ANSI compliant.
     *            Replace single quotation marks with double quotation marks.</li>
     *     <li>3. Convert FETCH NEXT n ROWS expression to prestodb-supported LIMIT n expression</li>
     * </ul>
     *
     * @param sqlQuery The sql query converted from druid query.
     * @return a presto compatible sql dialect.
     */
    protected static String sqlQueryToPrestoQuery(String sqlQuery) {
        if (sqlQuery == null || sqlQuery.isEmpty()) {
            throw new IllegalStateException("Input sqlQuery is null or empty");
        }

        // Extract the timestamp column name.
        String pat = ".*YEAR\\(\"(.*?)\"\\).*";
        Pattern pattern = Pattern.compile(pat, Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sqlQuery);
        matcher.matches();
        String timestampColumn = matcher.group(1);

        String fixVarcharCastQuery = sqlQuery.replace("CHARACTER SET \"ISO-8859-1\"", "");
        String fixTimePrestoQuery = fixVarcharCastQuery
                .replace(
                        String.format("DAYOFYEAR(\"%s\")", timestampColumn),
                        String.format("DAY_OF_YEAR(date_parse(SUBSTRING (%s,1,10),\'%%Y%%m%%d%%H\'))", timestampColumn)
                )
                .replace(
                        String.format(" YEAR(\"%s\")", timestampColumn),
                        String.format(" SUBSTRING(%s,1,4)", timestampColumn)
                )
                .replace(
                        String.format("MONTH(\"%s\")", timestampColumn),
                        String.format("SUBSTRING(%s,5,2)", timestampColumn)
                )
                .replace(
                        String.format("HOUR(\"%s\")", timestampColumn),
                        String.format("SUBSTRING(%s,9,2)", timestampColumn)
                );

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

        String fixFilterPrestoQuery = fixQuotePrestoQuery;
        return fetchToLimitHelper(fixFilterPrestoQuery);
    }

    /**
     * Helper function that converts FETCH NEXT into LIMIT expression
     * as Calcite converts limitspecs to FETCH NEXT
     * but prestodb does not have support for FETCH NEXT.
     *
     * @param fixQuotePrestoQuery The intermediate sqlToPrestoQuery.
     * @return a presto query with LIMIT fix.
     */
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
