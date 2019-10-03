/*
 * Copyright (c) 2019 Yahoo! Inc. All rights reserved.
 */
package com.yahoo.bard.webservice.sql.presto;


import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import com.facebook.presto.jdbc.PrestoDriver;

import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

/**
 *  DataSource to connection to HIVE tables throught Prestodb driver.
 * */
public class PrestoDataSource implements DataSource {
    private final Properties properties;
    public static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    public static final String DATABASE_URL = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("database_url")
    );
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(PrestoDataSource.class);

    /**
     * Constructor
     * Follow https://docs.google.com/presentation/d/17QfH9uF6JzTkbA8SSVhS-xwqKfpEEtgDxqYqMlK3B4s/edit#slide=id.p
     *
     * TODO: change user to headless
     */
    public PrestoDataSource() {
        properties = new Properties();
        properties.setProperty("user", "ymao");
        properties.setProperty("KerberosPrincipal", "ymao@Y.CORP.YAHOO.COM");
        properties.setProperty("KerberosUseCanonicalHostname", "false");
        properties.setProperty("KerberosRemoteServiceName", "HTTP");
        properties.setProperty("SSL", "true");
        properties.setProperty("SessionProperties", "distributed_join=false,query_max_execution_time=15m");
    }

    @Override
    public Connection getConnection() throws SQLException {
        DriverManager.registerDriver(new PrestoDriver());
        return DriverManager.getConnection(DATABASE_URL, properties);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {

    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {

    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
