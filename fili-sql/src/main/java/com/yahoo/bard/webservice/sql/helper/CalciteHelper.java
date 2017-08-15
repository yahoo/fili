// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.helper;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

/**
 * Small utility class to help with connection to databases, building, and writing sql.
 */
public class CalciteHelper {
    private final DataSource dataSource;
    private final SqlDialect dialect;

    /**
     * Initialize the helper with a datasource and it's schema.
     *
     * @param dataSource  The datasource to make connections with.
     *
     * @throws SQLException if failed while making a connection to the database.
     */
    public CalciteHelper(DataSource dataSource) throws SQLException {
        this.dataSource = dataSource;

        Connection connection = getConnection();
        this.dialect = SqlDialect.create(connection.getMetaData());
        connection.close();
    }

    /**
     * Creates a {@link RelBuilder} which can be used to build sql.
     *
     * @return a {@link RelBuilder} or null if an error occurred.
     */
    public RelBuilder getNewRelBuilder(String schemaName) {
        try {
            return getBuilder(dataSource, schemaName);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to build RelBuilder", e);
        }
    }

    /**
     * Creates a connection to the database from the {@link #dataSource}.
     *
     * @return the connection.
     *
     * @throws SQLException if can't create a connection.
     */
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    /**
     * Creates a new {@link RelToSqlConverter} using the dialect from
     * the datasource this was constructed with.
     *
     * @return a new converter.
     */
    public RelToSqlConverter getNewRelToSqlConverter() {
        return new RelToSqlConverter(dialect);
    }

    /**
     * Creates a new {@link SqlPrettyWriter} using the dialect from the
     * datasource this was constructed with.
     *
     * @return a new Sql writer
     */
    public SqlPrettyWriter getNewSqlWriter() {
        return new SqlPrettyWriter(dialect);
    }

    /**
     * Creates a {@link RelBuilder} with the given schema.
     *
     * @param dataSource  The dataSource for the jdbc schema.
     * @param schemaName  The name of the schema used for the database.
     *
     * @return the relbuilder from Calcite.
     *
     * @throws SQLException if can't readSqlResultSet from database.
     */
    public static RelBuilder getBuilder(DataSource dataSource, String schemaName) throws SQLException {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        return RelBuilder.create(
                Frameworks.newConfigBuilder()
                        .parserConfig(SqlParser.Config.DEFAULT)
                        .defaultSchema(addSchema(rootSchema, dataSource, schemaName))
                        .traitDefs((List<RelTraitDef>) null)
                        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2))
                        .build()
        );
    }

    /**
     * Adds the schema name to the rootSchema.
     *
     * @param rootSchema  The calcite schema for the database.
     * @param dataSource  The dataSource for the jdbc schema.
     * @param schemaName  The name of the schema used for the database.
     *
     * @return the schema.
     */
    private static SchemaPlus addSchema(SchemaPlus rootSchema, DataSource dataSource, String schemaName) {
        return rootSchema.add(// avg tests run at ~75-100ms
                schemaName,
                JdbcSchema.create(rootSchema, null, dataSource, null, null)
        );
        // todo look into timing/behavior of cloneschema (above is faster, but it could just be a result of H2)
        //        rootSchema.setCacheEnabled(true); //almost no effect
        //        return rootSchema.add( // avg tests run at ~200ms
        //                schemaName,
        //                new CloneSchema(
        //                        rootSchema.add(schemaName, JdbcSchema.create(rootSchema, null, dataSource, null,
        // null))
        //                )
        //        );
    }
}
