// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import com.yahoo.bard.webservice.data.cache.DataCache;
import com.yahoo.bard.webservice.data.config.ConfigurationLoader;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.dimension.TestDimensions;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.metric.TestMetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.data.config.table.TestTableLoader;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension;
import com.yahoo.bard.webservice.druid.client.DruidClientConfigHelper;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.client.impl.AsyncDruidWebServiceImpl;
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier;
import com.yahoo.bard.webservice.druid.util.FieldConverters;
import com.yahoo.bard.webservice.druid.util.SketchFieldConverter;
import com.yahoo.bard.webservice.metadata.QuerySigningService;
import com.yahoo.bard.webservice.metadata.TestDataSourceMetadataService;
import com.yahoo.bard.webservice.models.druid.client.impl.TestDruidWebService;
import com.yahoo.bard.webservice.web.FilteredSketchMetricsHelper;
import com.yahoo.bard.webservice.web.MetricsFilterSetBuilder;
import com.yahoo.bard.webservice.web.filters.TestLogWrapperFilter;

import com.codahale.metrics.jersey2.InstrumentedResourceMethodApplicationListener;
import com.codahale.metrics.logback.InstrumentedAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.datatype.joda.JodaModule;

import org.glassfish.hk2.api.MultiException;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.ws.rs.core.Application;

/**
 * Configures JerseyTest and also sets up DI.  This is a singleton since JerseyTest binds a network port.
 */
public class JerseyTestBinder {
    private static final Logger LOG = LoggerFactory.getLogger(JerseyTestBinder.class);

    public ApplicationState state;

    public AbstractBinder binder;
    public ResourceConfig config;
    public JerseyTest harness;
    public ConfigurationLoader configurationLoader;
    public TestBinderFactory testBinderFactory;
    public boolean useTestWebService = true;
    private static final String RANDOM_PORT = "0";

    private DateTimeZone previousDateTimeZone;

    private long startTimeout = 30000L;

    public TestDimensions testDimensions = new TestDimensions();

    /**
     * Constructor that will auto-start.
     *
     * @param resourceClasses  Resource classes for Jersey to load
     */
    public JerseyTestBinder(Class<?>... resourceClasses) {
        this(true, new ApplicationState(), resourceClasses);
    }

    /**
     * Constructor.
     *
     * @param doStart  Flag to indicate if the constructor should start the test harness
     * @param resourceClasses  Resource classes for Jersey to load
     */
    public JerseyTestBinder(boolean doStart, Class<?>... resourceClasses) {
        this(doStart, new ApplicationState(), resourceClasses);
    }

    /**
     * Constructor with more control over auto-start and the application state it uses.
     *
     * @param doStart  Will auto-start test harness after constructing if true, must be manually started if false.
     * @param state  Application state to load for testing
     * @param resourceClasses  Resource classes for Jersey to load
     */
    public JerseyTestBinder(boolean doStart, ApplicationState state, Class<?>... resourceClasses) {

        this.state = state;

        //Initializing the Sketch field converter
        FieldConverterSupplier.sketchConverter = initializeSketchConverter();

        //Initialize the metrics filter helper
        FieldConverterSupplier.metricsFilterSetBuilder = initializeMetricsFilterSetBuilder();

        // Set up the web services
        buildWebServices();

        // Pin the default timezone to UTC so that we use the same timezone no matter where tests run
        previousDateTimeZone = DateTimeZone.getDefault();
        DateTimeZone.setDefault(DateTimeZone.UTC);

        // Fill in the binder factory
        testBinderFactory = buildBinderFactory(
                getDimensionConfiguration(),
                getMetricLoader(),
                getTableLoader(),
                state
        );
        this.binder = (AbstractBinder) (testBinderFactory.buildBinder());

        // Configure and register the resources
        this.config = new ResourceConfig();

        // Order matters. First check if BardLoggingFilter is requested
        boolean skipWrapper = false;
        for (Class<?> cls : resourceClasses) {
            if (cls.getSimpleName().equals("BardLoggingFilter")) {
                skipWrapper = true;
            }
        }

        // If BardLoggingFilter is not requested, use a wrapper instead to enable logging of the information that is
        // potentially recorded in the resources that are registered
        if (skipWrapper) {
            this.config.registerClasses(resourceClasses);
        } else {
            this.config.register(TestLogWrapperFilter.class, 1);
            // Now register the requested classes
            for (Class<?> cls : resourceClasses) {
                this.config.register(cls, 5);
            }
        }

        this.config.register(new InstrumentedResourceMethodApplicationListener(MetricRegistryFactory.getRegistry()));
        this.config.register(this.binder);

        registerMetricsAppender();

        // Create and set up the test harness
        this.harness = new JerseyTest() {
            @Override
            protected Application configure() {
                // Find first available port.
                forceSet(TestProperties.CONTAINER_PORT, RANDOM_PORT);

                return config;
            }
        };

        if (doStart) {
            start();
        }
    }

    /**
     * Build a test binder factory.
     *
     * @param dimensionConfiguration  Dimensions to load
     * @param metricLoader  Metrics to load
     * @param tableLoader  Tables to load
     * @param state  Application state to load
     *
     * @return a configured TestBinderFactory
     */
    public TestBinderFactory buildBinderFactory(
            LinkedHashSet<DimensionConfig> dimensionConfiguration,
            MetricLoader metricLoader,
            TableLoader tableLoader,
            ApplicationState state
    ) {
        return new TestBinderFactory(dimensionConfiguration, metricLoader, tableLoader, state);
    }

    /**
     * Create a Codahale metric appender and add to ROOT logger.
     */
    private void registerMetricsAppender() {
        // Get the root logger
        LoggerContext factory = (LoggerContext) LoggerFactory.getILoggerFactory();
        ch.qos.logback.classic.Logger rootLogger = factory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);

        // Create the metrics appender and give it the root logger's context
        InstrumentedAppender appender = new InstrumentedAppender(MetricRegistryFactory.getRegistry());
        appender.setContext(rootLogger.getLoggerContext());
        appender.start();

        // Add the appender to the root logger
        rootLogger.addAppender(appender);
    }

    /**
     * Start the test harness.
     */
    public void start() {
        try {
            new StartHarness().startHarness();
        } catch (Exception e) {
            throw (e instanceof IllegalStateException) ? (IllegalStateException) e : new IllegalStateException(e);
        }
    }

    /**
     * Start harness and wait.
     */
    private class StartHarness extends Thread {
        private Throwable cause;

        /**
         * Start harness and wait.
         *
         * @throws InterruptedException thread was interrupted
         */
        private StartHarness() throws InterruptedException {
            super("Start Harness");
        }

        /**
         * Start the test harness and track it for timeouts.
         *
         * @throws InterruptedException if the harness is interrupted
         */
        public void startHarness() throws InterruptedException {
            this.start();
            this.join(startTimeout);
            // If harness not started, throw timeout exception
            if (isAlive()) {
                // Include thread stack dump
                StringBuilder sb = new StringBuilder("Timeout starting Jersey\n");
                for (StackTraceElement ste : this.getStackTrace()) {
                    sb.append("\tat ").append(ste).append('\n');
                }
                // try to interrupt and tear down
                this.interrupt();
                this.join(10000);
                if (!isAlive()) {
                    try {
                        harness.tearDown();
                    } catch (Exception e) {
                        throw new IllegalStateException(sb.toString(), e);
                    }
                }
                throw new IllegalStateException(sb.toString(), cause);
            }
            // If problem starting harness, throw cause
            if (cause != null) {
                throw new IllegalStateException(cause);
            }
        }

        @Override
        public void run() {
            try {
                harness.setUp();
                configurationLoader = testBinderFactory.getConfigurationLoader();
            } catch (Throwable e) {
                cause = e;
            }
        }
    }

    /**
     * Initialize the field converter. By default it is SketchFieldConverter
     *
     * @return An instance of SketchFieldConverter
     */
    protected FieldConverters initializeSketchConverter() {
        return new SketchFieldConverter();
    }

    /**
     * Initialize the FilteredMetricsHelper. By default it is FilteredSketchMetricsHelper
     *
     * @return An instance of FilteredSketchMetricsHelper
     */
    protected MetricsFilterSetBuilder initializeMetricsFilterSetBuilder() {
        return new FilteredSketchMetricsHelper();
    }

    public ConfigurationLoader getConfigurationLoader() {
        return configurationLoader;
    }

    public AbstractBinder getBinder() {
        return binder;
    }

    public JerseyTest getHarness() {
        return harness;
    }

    public DruidWebService getDruidWebService() {
        return state.webService;
    }

    public DruidWebService getMetadataDruidWebService() {
        return state.metadataWebService;
    }

    public DataCache<?> getDataCache() {
        return state.cache;
    }

    public void setDataCache(DataCache<?> cache) {
        state.cache = cache;
    }

    public QuerySigningService<?> getQuerySigningService() {
        return state.querySigningService;
    }

    public void setQuerySigningService(QuerySigningService<?> querySigningService) {
        state.querySigningService = querySigningService;
    }

    /**
     * Tear down the test harness and unload the binder.
     *
     * @throws Exception if there's a problem tearing things down
     */
    public void tearDown() throws Exception {

        // Reset the default timezone to what it was before the JTB was created
        DateTimeZone.setDefault(previousDateTimeZone);

        getHarness().tearDown();

        DimensionDictionary dictionary = configurationLoader.getDimensionDictionary();
        Set<Dimension> dimensions = dictionary.findAll();
        List<Throwable> caughtExceptions = Collections.emptyList();
        for (Dimension dimension : dimensions) {
            if (dimension instanceof KeyValueStoreDimension) {
                KeyValueStoreDimension kvDimension = (KeyValueStoreDimension) dimension;
                try {
                    kvDimension.deleteAllDimensionRows();
                } catch (Exception e) {
                    caughtExceptions.add(e);
                    String msg = String.format("Unable to delete all DimensionRows for %s", dimension.getApiName());
                    LOG.error(msg, e);
                }
            }
        }
        state.cache.clear();
        testBinderFactory.shutdownLoaderScheduler();

        if (!caughtExceptions.isEmpty()) {
            // Throw what we caught last so that we don't lose it.
            throw new MultiException(caughtExceptions);
        }

        getDimensionConfiguration().stream()
                .map(DimensionConfig::getSearchProvider)
                .forEach(SearchProvider::clearDimension);
    }

    public boolean isUseTestWebService() {
        return useTestWebService;
    }

    public void setUseTestWebService(boolean useTestWebService) {
        this.useTestWebService = useTestWebService;
    }

    public LinkedHashSet<DimensionConfig> getDimensionConfiguration() {
        return testDimensions.getAllDimensionConfigurations();
    }

    public MetricLoader getMetricLoader() {
        return new TestMetricLoader();
    }

    public TableLoader getTableLoader() {
        return new TestTableLoader(new TestDataSourceMetadataService());
    }

    /**
     * Build the web services to use and assign them to the application state.
     */
    protected void buildWebServices() {
        // Build an ObjectMapper for everyone to use, since they are heavy-weight
        ObjectMapper mapper = new ObjectMapper();
        JodaModule jodaModule = new JodaModule();
        jodaModule.addSerializer(Interval.class, new ToStringSerializer());
        mapper.registerModule(jodaModule);

        // This alternate switched implementation approach is not really used anywhere, should be split off into a
        // separate subclass if needed
        if (state.webService == null) {
            state.webService = (useTestWebService) ?
                    new TestDruidWebService("Test UI WS") :
                    new AsyncDruidWebServiceImpl(DruidClientConfigHelper.getServiceConfig(), mapper);
        }
        if (state.metadataWebService == null) {
            state.metadataWebService = (useTestWebService) ?
                    new TestDruidWebService("Test Metadata WS") :
                    new AsyncDruidWebServiceImpl(DruidClientConfigHelper.getMetadataServiceConfig(), mapper);
        }
    }
}
