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
    public static final String DEFAULT_SCHEMA = "PUBLIC";
    private final DataSource dataSource;
    private final String schemaName;

    /**
     * Initialize the helper with a datasource and it's schema.
     *
     * @param dataSource  The datasource to make connections with.
     * @param schemaName  The name of the schema where the tables are stored.
     *
     * @throws SQLException if failed while making a connection to the database.
     */
    public CalciteHelper(DataSource dataSource, String schemaName) {
        this.dataSource = dataSource;
        this.schemaName = schemaName;
    }

    /**
     * Creates a {@link RelBuilder} which can be used to build sql.
     *
     * @return a {@link RelBuilder} or null if an error occurred.
     */
    public RelBuilder getNewRelBuilder() {
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

    public RelToSqlConverter getNewRelToSqlConverter() throws SQLException {
        Connection connection = getConnection();
        RelToSqlConverter relToSqlConverter = new RelToSqlConverter(SqlDialect.create(connection.getMetaData()));
        connection.close();
        return relToSqlConverter;
    }

    /**
     * Creates a {@link RelBuilder} with a root scema of {@link #DEFAULT_SCHEMA}.
     *
     * @param dataSource  The dataSource for the jdbc schema.
     *
     * @return the relbuilder from Calcite.
     *
     * @throws SQLException if can't readSqlResultSet from database.
     */
    public static RelBuilder getBuilder(DataSource dataSource) throws SQLException {
        return getBuilder(dataSource, DEFAULT_SCHEMA);
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

    public SqlPrettyWriter getNewSqlWriter() throws SQLException {
        Connection connection = getConnection();
        SqlPrettyWriter sqlPrettyWriter = new SqlPrettyWriter(SqlDialect.create(connection.getMetaData()));
        connection.close();
        return sqlPrettyWriter;
    }

    /**
     * Gets the name of the schema that the calcite was configured with.
     *
     * @return the schema name (i.e. {@link #DEFAULT_SCHEMA})
     */
    public String getSchemaName() {
        return schemaName;
    }
}
