package com.yahoo.bard.webservice.sql;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

/**
 * Created by hinterlong on 6/20/17.
 */
public class CalciteHelper {
    public static final String DEFAULT_SCHEMA = "PUBLIC";
    private final DataSource dataSource;
    private final String username;
    private final String password;
    private final String schemaName;

    public CalciteHelper(DataSource dataSource) {
        this(dataSource, null, null, DEFAULT_SCHEMA);
    }

    public CalciteHelper(DataSource dataSource, String schemaName) {
        this(dataSource, null, null, schemaName);
    }

    public CalciteHelper(DataSource dataSource, String username, String password, String schemaName) {
        this.dataSource = dataSource;
        this.username = username;
        this.password = password;
        this.schemaName = schemaName;
    }

    public RelBuilder getNewRelBuilder() {
        try {
            return getBuilder(dataSource, schemaName);
        } catch (SQLException ignored) {
        }
        return null;
    }

    public Connection getConnection() throws SQLException {
        if (username == null || password == null) {
            return dataSource.getConnection();
        } else {
            return dataSource.getConnection(username, password);
        }
    }

    public RelToSqlConverter getNewRelToSqlConverter() throws SQLException {
        return new RelToSqlConverter(SqlDialect.create(getConnection().getMetaData()));
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
        // todo look into cloning schema
        return rootSchema.add(
                schemaName,
                JdbcSchema.create(rootSchema, null, dataSource, null, null)
        );
    }
}
