// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.presto;


import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import com.facebook.presto.jdbc.NotImplementedException;
import com.facebook.presto.jdbc.PrestoDriver;

import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
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
    public static final String USER = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("database_user")
    );
    public static final String KERBEROS_PRINCIPAL = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("database_kerberos_principal")
    );
    public static final String KERBEROS_KEYTAB_PATH = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("database_kerberos_keytab_path")
    );
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(PrestoDataSource.class);
    private Driver driver;

    /**
     * Constructor.
     */
    public PrestoDataSource() {
        properties = new Properties();
        properties.setProperty("user", USER);
        properties.setProperty("SSL", "true");
        properties.setProperty("KerberosRemoteServiceName", "HTTP");
        properties.setProperty("KerberosUseCanonicalHostname", "false");
        properties.setProperty("KerberosConfigPath", "/etc/krb5.conf");
        properties.setProperty("KerberosPrincipal", KERBEROS_PRINCIPAL);
        properties.setProperty("KerberosKeytabPath", KERBEROS_KEYTAB_PATH);
        this.driver = new PrestoDriver();
    }

    /**
     * Constructor.
     * @param properties the properties of the connection
     * @param driver the jdbc driver
     */
    public PrestoDataSource(Properties properties, Driver driver) {
        this.properties = properties;
        this.driver = driver;
    }

    @Override
    public Connection getConnection() throws SQLException {
        DriverManager.registerDriver(driver);
        LOG.info("getConnection with properties: {}", properties);
        return DriverManager.getConnection(DATABASE_URL, properties);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        throw new NotImplementedException("PrestoDataSource", "getConnection");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new NotImplementedException("PrestoDataSource", "unwrap");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new NotImplementedException("PrestoDataSource", "isWrapperFor");
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        throw new NotImplementedException("PrestoDataSource", "getLogWriter");
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        throw new NotImplementedException("PrestoDataSource", "setLogWriter");
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        throw new NotImplementedException("PrestoDataSource", "setLoginTimeout");
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        throw new NotImplementedException("PrestoDataSource", "getLoginTimeout");
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
