// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import static com.yahoo.bard.webservice.config.BardFeatureFlag.DRUID_COORDINATOR_METADATA;
import static com.yahoo.bard.webservice.config.BardFeatureFlag.DRUID_DIMENSIONS_LOADER;
import static com.yahoo.bard.webservice.config.BardFeatureFlag.DRUID_LOOKUP_METADATA;
import static com.yahoo.bard.webservice.web.handlers.CacheRequestHandler.CACHE_HITS;
import static com.yahoo.bard.webservice.web.handlers.CacheRequestHandler.CACHE_REQUESTS;
import static com.yahoo.bard.webservice.web.handlers.DefaultWebServiceHandlerSelector.QUERY_REQUEST_TOTAL;
import static com.yahoo.bard.webservice.web.handlers.SplitQueryRequestHandler.SPLITS;
import static com.yahoo.bard.webservice.web.handlers.SplitQueryRequestHandler.SPLIT_QUERIES;

import com.yahoo.bard.webservice.application.healthchecks.AllDimensionsLoadedHealthCheck;
import com.yahoo.bard.webservice.application.healthchecks.DataSourceMetadataLoaderHealthCheck;
import com.yahoo.bard.webservice.application.healthchecks.DruidDimensionsLoaderHealthCheck;
import com.yahoo.bard.webservice.application.healthchecks.LookupHealthCheck;
import com.yahoo.bard.webservice.application.healthchecks.VersionHealthCheck;
import com.yahoo.bard.webservice.async.broadcastchannels.BroadcastChannel;
import com.yahoo.bard.webservice.async.broadcastchannels.SimpleBroadcastChannel;
import com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField;
import com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobRowBuilder;
import com.yahoo.bard.webservice.async.jobs.jobrows.JobRowBuilder;
import com.yahoo.bard.webservice.async.jobs.payloads.DefaultJobPayloadBuilder;
import com.yahoo.bard.webservice.async.jobs.payloads.JobPayloadBuilder;
import com.yahoo.bard.webservice.async.jobs.stores.ApiJobStore;
import com.yahoo.bard.webservice.async.jobs.stores.NoOpApiJobStore;
import com.yahoo.bard.webservice.async.preresponses.stores.NoOpPreResponseStore;
import com.yahoo.bard.webservice.async.preresponses.stores.PreResponseStore;
import com.yahoo.bard.webservice.async.workflows.AsynchronousWorkflowsBuilder;
import com.yahoo.bard.webservice.async.workflows.DefaultAsynchronousWorkflowsBuilder;
import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.config.CacheFeatureFlag;
import com.yahoo.bard.webservice.config.FeatureFlag;
import com.yahoo.bard.webservice.config.FeatureFlagRegistry;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.DruidQueryBuilder;
import com.yahoo.bard.webservice.data.DruidResponseParser;
import com.yahoo.bard.webservice.data.HttpResponseMaker;
import com.yahoo.bard.webservice.data.PartialDataHandler;
import com.yahoo.bard.webservice.data.PreResponseDeserializer;
import com.yahoo.bard.webservice.data.cache.DataCache;
import com.yahoo.bard.webservice.data.cache.HashDataCache;
import com.yahoo.bard.webservice.data.cache.MemDataCache;
import com.yahoo.bard.webservice.data.cache.MemTupleDataCache;
import com.yahoo.bard.webservice.data.cache.StubDataCache;
import com.yahoo.bard.webservice.data.config.ConfigurationLoader;
import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.dimension.DimensionLoader;
import com.yahoo.bard.webservice.data.config.dimension.TypeAwareDimensionLoader;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQueryMerger;
import com.yahoo.bard.webservice.data.time.GranularityDictionary;
import com.yahoo.bard.webservice.data.time.GranularityParser;
import com.yahoo.bard.webservice.data.time.StandardGranularityParser;
import com.yahoo.bard.webservice.data.volatility.DefaultingVolatileIntervalsService;
import com.yahoo.bard.webservice.data.volatility.VolatileIntervalsService;
import com.yahoo.bard.webservice.druid.client.DruidClientConfigHelper;
import com.yahoo.bard.webservice.druid.client.DruidServiceConfig;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.client.impl.AsyncDruidWebServiceImpl;
import com.yahoo.bard.webservice.druid.client.impl.HeaderNestingJsonBuilderStrategy;
import com.yahoo.bard.webservice.druid.model.builders.DefaultDruidHavingBuilder;
import com.yahoo.bard.webservice.druid.model.builders.DruidFilterBuilder;
import com.yahoo.bard.webservice.druid.model.builders.DruidHavingBuilder;
import com.yahoo.bard.webservice.druid.model.builders.DruidInFilterBuilder;
import com.yahoo.bard.webservice.druid.model.builders.DruidOrFilterBuilder;
import com.yahoo.bard.webservice.druid.model.query.LookbackQuery;
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier;
import com.yahoo.bard.webservice.druid.util.FieldConverters;
import com.yahoo.bard.webservice.druid.util.ThetaSketchFieldConverter;
import com.yahoo.bard.webservice.exception.DataExceptionHandler;
import com.yahoo.bard.webservice.exception.FiliDataExceptionHandler;
import com.yahoo.bard.webservice.exception.FiliDimensionExceptionHandler;
import com.yahoo.bard.webservice.exception.FiliJobsExceptionHandler;
import com.yahoo.bard.webservice.exception.FiliMetricExceptionHandler;
import com.yahoo.bard.webservice.exception.FiliSlicesExceptionHandler;
import com.yahoo.bard.webservice.exception.FiliTablesExceptionHandler;
import com.yahoo.bard.webservice.exception.MetadataExceptionHandler;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataLoadTask;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.metadata.QuerySigningService;
import com.yahoo.bard.webservice.metadata.RegisteredLookupMetadataLoadTask;
import com.yahoo.bard.webservice.metadata.RequestedIntervalsFunction;
import com.yahoo.bard.webservice.metadata.SegmentIntervalsHashIdGenerator;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.bard.webservice.table.resolver.DefaultPhysicalTableResolver;
import com.yahoo.bard.webservice.table.resolver.PhysicalTableResolver;
import com.yahoo.bard.webservice.util.DefaultingDictionary;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.web.CsvResponseWriter;
import com.yahoo.bard.webservice.web.DefaultResponseFormatResolver;
import com.yahoo.bard.webservice.web.DimensionApiRequestMapper;
import com.yahoo.bard.webservice.web.FiliResponseWriter;
import com.yahoo.bard.webservice.web.FiliResponseWriterSelector;
import com.yahoo.bard.webservice.web.FilteredThetaSketchMetricsHelper;
import com.yahoo.bard.webservice.web.JsonApiResponseWriter;
import com.yahoo.bard.webservice.web.JsonResponseWriter;
import com.yahoo.bard.webservice.web.MetricsFilterSetBuilder;
import com.yahoo.bard.webservice.web.NoOpRequestMapper;
import com.yahoo.bard.webservice.web.RateLimiter;
import com.yahoo.bard.webservice.web.RequestMapper;
import com.yahoo.bard.webservice.web.ResponseFormatResolver;
import com.yahoo.bard.webservice.web.ResponseWriter;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestFactory;
import com.yahoo.bard.webservice.web.apirequest.DefaultDataApiRequestFactory;
import com.yahoo.bard.webservice.web.apirequest.DimensionsApiRequest;
import com.yahoo.bard.webservice.web.apirequest.JobsApiRequest;
import com.yahoo.bard.webservice.web.apirequest.MetricsApiRequest;
import com.yahoo.bard.webservice.web.apirequest.SlicesApiRequest;
import com.yahoo.bard.webservice.web.apirequest.TablesApiRequest;
import com.yahoo.bard.webservice.web.apirequest.binders.DefaultHavingApiGenerator;
import com.yahoo.bard.webservice.web.apirequest.binders.HavingGenerator;
import com.yahoo.bard.webservice.web.apirequest.binders.PerRequestDictionaryHavingGenerator;
import com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow;
import com.yahoo.bard.webservice.web.handlers.workflow.RequestWorkflowProvider;
import com.yahoo.bard.webservice.web.ratelimit.DefaultRateLimiter;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessorFactory;
import com.yahoo.bard.webservice.web.responseprocessors.ResultSetResponseProcessorFactory;
import com.yahoo.bard.webservice.web.util.QueryWeightUtil;
import com.yahoo.bard.webservice.web.util.ResponseUtils;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.Binder;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.subjects.PublishSubject;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.time.Clock;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.validation.constraints.NotNull;

/**
 *   Abstract Binder factory implements the standard buildBinder functionality.
 *   It is left to individual projects to subclass, providing dimensions config,
 *   Metric and Table loading classes.
 */
public abstract class AbstractBinderFactory implements BinderFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBinderFactory.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    public static final String HEALTH_CHECK_NAME_DATASOURCE_METADATA = "datasource metadata loader";
    public static final String HEALTH_CHECK_NAME_LOOKUP_METADATA = "lookup metadata loader";
    public static final String HEALTH_CHECK_NAME_DRUID_DIM_LOADER = "druid dimensions loader";
    public static final String HEALTH_CHECK_VERSION = "version";
    public static final String HEALTH_CHECK_NAME_DIMENSION = "dimension check";

    private static final String METER_CACHE_HIT_RATIO = "queries.meter.cache.hit_ratio";
    private static final String METER_SPLITS_TOTAL_RATIO = "queries.meter.split_queries.total_ratio";
    private static final String METER_SPLITS_RATIO = "queries.meter.split_queries.ratio";

    private static final String JVM_UPTIME = "jvm.uptime";

    private static final String DRUID_HEADER_SUPPLIER_CLASS = "druid_header_supplier_class";

    // Two minutes in milliseconds
    public static final int HC_LAST_RUN_PERIOD_MILLIS_DEFAULT = 120 * 1000;
    public static final int LOADER_SCHEDULER_THREAD_POOL_SIZE_DEFAULT = 4;

    public static final int SEG_LOADER_HC_LAST_RUN_PERIOD_MILLIS = SYSTEM_CONFIG.getIntProperty(
            SYSTEM_CONFIG.getPackageVariableName("seg_loader_health_check_last_run_duration"),
            HC_LAST_RUN_PERIOD_MILLIS_DEFAULT
    );

    public static final int DRUID_DIM_LOADER_HC_LAST_RUN_PERIOD_MILLIS = SYSTEM_CONFIG.getIntProperty(
            SYSTEM_CONFIG.getPackageVariableName("druid_dim_loader_health_check_last_run_duration"),
            HC_LAST_RUN_PERIOD_MILLIS_DEFAULT
    );

    public static final int LOADER_SCHEDULER_THREAD_POOL_SIZE = SYSTEM_CONFIG.getIntProperty(
            SYSTEM_CONFIG.getPackageVariableName("loader_scheduler_thread_pool_size"),
            LOADER_SCHEDULER_THREAD_POOL_SIZE_DEFAULT
    );

    public static final String DEPRECATED_PERMISSIVE_AVAILABILITY_FLAG = SYSTEM_CONFIG.getPackageVariableName(
            "permissive_column_availability_enabled");

    public static final int DRUID_UNCOVERED_INTERVAL_LIMIT = SYSTEM_CONFIG.getIntProperty(
            SYSTEM_CONFIG.getPackageVariableName("druid_uncovered_interval_limit"),
            0
    );

    public static final String SYSTEM_CONFIG_TIMEZONE_KEY = "timezone";

    private ObjectMappersSuite objectMappers;

    private DataSourceMetadataService dataSourceMetadataService;
    private ConfigurationLoader loader;

    private final TaskScheduler loaderScheduler = new TaskScheduler(LOADER_SCHEDULER_THREAD_POOL_SIZE);

    /**
     * Constructor.
     */
    public AbstractBinderFactory() {
        super();
    }

    @Override
    public final Binder buildBinder() {
        return new AbstractBinder() {
            @Override
            protected void configure() {
                HealthCheckRegistry healthCheckRegistry = HealthCheckRegistryFactory.getRegistry();

                Stream.concat(
                        collectFeatureFlags(BardFeatureFlag.class).stream(),
                        collectFeatureFlags().stream()
                ).forEach(flag -> bind(flag).named(flag.name()).to(FeatureFlag.class));

                bind(FeatureFlagRegistry.class).to(FeatureFlagRegistry.class);

                // Bard currently expects there to be one web service
                DruidWebService druidWebService = buildDruidWebService(getMappers().getMapper());

                bind(druidWebService).to(DruidWebService.class);

                // A separate web service for metadata
                DruidWebService metadataDruidWebService = null;
                if (DRUID_COORDINATOR_METADATA.isOn()) {
                    metadataDruidWebService = buildMetadataDruidWebService(getMappers().getMapper());
                    bind(metadataDruidWebService).named("metadataDruidWebService").to(DruidWebService.class);
                }

                // Bind the timeGrain
                bind(getGranularityDictionary()).to(GranularityDictionary.class);
                bind(getGranularityParser()).to(GranularityParser.class);

                // Bind the request building action classes for druid
                bind(buildDruidQueryBuilder()).to(DruidQueryBuilder.class);
                bind(TemplateDruidQueryMerger.class).to(TemplateDruidQueryMerger.class);
                bind(buildDruidResponseParser()).to(DruidResponseParser.class);
                bind(buildDruidFilterBuilder()).to(DruidFilterBuilder.class);
                bind(buildDruidHavingBuilder()).to(DruidHavingBuilder.class);

                bind(buildDataApiRequestFactory()).to(DataApiRequestFactory.class);

                //Initialize the field converter
                FieldConverterSupplier.sketchConverter = initializeSketchConverter();

                //Initialize the metrics filter helper
                FieldConverterSupplier.metricsFilterSetBuilder = initializeMetricsFilterSetBuilder();

                // Build the datasource metadata service containing the data segments
                bind(getDataSourceMetadataService()).to(DataSourceMetadataService.class);

                // Build the configuration loader and load configuration
                loader = getConfigurationLoader();
                loader.load();
                bindDictionaries(this);
                bind(buildHavingGenerator(loader)).to(HavingGenerator.class);

                // Bind the request mappers
                bindRequestMappers(this);

                // Setup end points and back end services
                setupHealthChecks(healthCheckRegistry, loader.getDimensionDictionary());
                setupGauges();

                bind(buildCache()).to(DataCache.class);
                bind(QueryWeightUtil.class).to(QueryWeightUtil.class);

                bind(getMappers()).to(ObjectMappersSuite.class);
                bind(getMappers().getMapper()).to(ObjectMapper.class);

                bind(getWorkflow()).to(RequestWorkflowProvider.class);
                bind(getPhysicalTableResolver()).to(PhysicalTableResolver.class);
                bind(PartialDataHandler.class).to(PartialDataHandler.class);
                bind(getVolatileIntervalsService()).to(VolatileIntervalsService.class);

                QuerySigningService<?> querySigningService = buildQuerySigningService(
                        loader.getPhysicalTableDictionary(),
                        dataSourceMetadataService
                );

                if (DRUID_COORDINATOR_METADATA.isOn()) {
                    DataSourceMetadataLoadTask dataSourceMetadataLoader = buildDataSourceMetadataLoader(
                            metadataDruidWebService,
                            loader.getPhysicalTableDictionary(),
                            dataSourceMetadataService,
                            getMappers().getMapper()
                    );

                    setupDataSourceMetaData(healthCheckRegistry, dataSourceMetadataLoader);
                }

                if (DRUID_LOOKUP_METADATA.isOn()) {
                    setupLookUpMetadataLoader(
                            healthCheckRegistry,
                            buildLookupMetaDataLoader(metadataDruidWebService, loader.getDimensionDictionary())
                    );
                }

                bind(querySigningService).to(QuerySigningService.class);

                bind(buildJobRowBuilder()).to(JobRowBuilder.class);

                bind(buildPreResponseStore(loader.getDictionaries())).to(PreResponseStore.class);

                bind(buildBroadcastChannel()).to(new TypeLiteral<BroadcastChannel<String>>() { });

                bind(buildApiJobStore()).to(ApiJobStore.class);

                bind(buildJobPayloadBuilder()).to(JobPayloadBuilder.class);

                bind(getAsynchronousProcessBuilder()).to(AsynchronousWorkflowsBuilder.class);

                bind(getClock()).to(Clock.class);

                bind(getHttpResponseMaker()).to(HttpResponseMaker.class);

                bind(buildResponseUtils()).to(ResponseUtils.class);

                bind(buildResponseWriter(getMappers())).to(ResponseWriter.class);

                bind(buildResponseFormatResolver()).to(ResponseFormatResolver.class);

                bind(buildResponseProcessorFactory()).to(ResponseProcessorFactory.class);

                bind(buildRateLimiter()).to(RateLimiter.class);

                bind(getDataExceptionHandler()).to(DataExceptionHandler.class);

                bindExceptionHandlers(this);

                if (DRUID_DIMENSIONS_LOADER.isOn()) {
                    DimensionValueLoadTask dimensionLoader = buildDruidDimensionsLoader(
                            druidWebService,
                            loader.getPhysicalTableDictionary(),
                            loader.getDimensionDictionary()
                    );
                    setupDruidDimensionsLoader(healthCheckRegistry, dimensionLoader);
                }
                if (SYSTEM_CONFIG.getBooleanProperty(DEPRECATED_PERMISSIVE_AVAILABILITY_FLAG, false)) {
                    LOG.warn(
                            "Permissive column availability feature flag is no longer supported, please use " +
                                    "PermissivePhysicalTable to enable permissive column availability."
                    );
                }
                // Call post-binding hook to allow for additional binding
                afterBinding(this);
            }

        };
    }

    /**
     * Binds all the resource dictionaries.
     *
     * @param binder The binder to bind the dictionaries to.
     */
    private void bindDictionaries(AbstractBinder binder) {
        // Bind the configuration dictionaries
        binder.bind(loader.getDimensionDictionary()).to(DimensionDictionary.class);
        binder.bind(loader.getMetricDictionary()).to(MetricDictionary.class);
        binder.bind(loader.getLogicalTableDictionary()).to(LogicalTableDictionary.class);
        binder.bind(loader.getPhysicalTableDictionary()).to(PhysicalTableDictionary.class);
        binder.bind(loader.getDictionaries()).to(ResourceDictionaries.class);
    }

    /**
     * Binds all the exception handlers to the specified binder.
     *
     * @param binder  The binder to bind the exception handlers to
     */
    protected void bindExceptionHandlers(AbstractBinder binder) {
        binder.bind(getDimensionExceptionHandler())
                .named(DimensionsApiRequest.EXCEPTION_HANDLER_NAMESPACE)
                .to(MetadataExceptionHandler.class);

        binder.bind(getMetricsExceptionHandler())
                .named(MetricsApiRequest.EXCEPTION_HANDLER_NAMESPACE)
                .to(MetadataExceptionHandler.class);

        binder.bind(getTablesExceptionHandler())
                .named(TablesApiRequest.EXCEPTION_HANDLER_NAMESPACE)
                .to(MetadataExceptionHandler.class);

        binder.bind(getSlicesExceptionHandler())
                .named(SlicesApiRequest.EXCEPTION_HANDLER_NAMESPACE)
                .to(MetadataExceptionHandler.class);

        binder.bind(getJobsExceptionHandler())
                .named(JobsApiRequest.EXCEPTION_HANDLER_NAMESPACE)
                .to(MetadataExceptionHandler.class);

    }

    protected Class<? extends MetadataExceptionHandler> getDimensionExceptionHandler() {
        return FiliDimensionExceptionHandler.class;
    }

    protected Class<? extends MetadataExceptionHandler> getMetricsExceptionHandler() {
        return FiliMetricExceptionHandler.class;
    }

    protected Class<? extends MetadataExceptionHandler> getSlicesExceptionHandler() {
        return FiliSlicesExceptionHandler.class;
    }

    protected Class<? extends MetadataExceptionHandler> getTablesExceptionHandler() {
        return FiliTablesExceptionHandler.class;
    }

    protected Class<? extends MetadataExceptionHandler> getJobsExceptionHandler() {
        return FiliJobsExceptionHandler.class;
    }

    protected Class<? extends DataExceptionHandler> getDataExceptionHandler() {
        return FiliDataExceptionHandler.class;
    }

    /**
     * Bind ApiRequest instances to resource scope names.
     *
     * @param binder  The binder being used to bind the request mappers.
     */
    private void bindRequestMappers(AbstractBinder binder) {
        Map<String, RequestMapper> requestMappers = getRequestMappers(loader.getDictionaries());
        binder.bind(requestMappers.getOrDefault(
                DimensionsApiRequest.REQUEST_MAPPER_NAMESPACE,
                new DimensionApiRequestMapper(loader.getDictionaries())
        )).named(DimensionsApiRequest.REQUEST_MAPPER_NAMESPACE).to(RequestMapper.class);

        binder.bind(requestMappers.getOrDefault(
                MetricsApiRequest.REQUEST_MAPPER_NAMESPACE,
                new NoOpRequestMapper(loader.getDictionaries())
        )).named(MetricsApiRequest.REQUEST_MAPPER_NAMESPACE).to(RequestMapper.class);

        binder.bind(requestMappers.getOrDefault(
                SlicesApiRequest.REQUEST_MAPPER_NAMESPACE,
                new NoOpRequestMapper(loader.getDictionaries())
        )).named(SlicesApiRequest.REQUEST_MAPPER_NAMESPACE).to(RequestMapper.class);

        binder.bind(requestMappers.getOrDefault(
                TablesApiRequest.REQUEST_MAPPER_NAMESPACE,
                new NoOpRequestMapper(loader.getDictionaries())
        )).named(TablesApiRequest.REQUEST_MAPPER_NAMESPACE).to(RequestMapper.class);

        binder.bind(requestMappers.getOrDefault(
                DataApiRequest.REQUEST_MAPPER_NAMESPACE,
                new NoOpRequestMapper(loader.getDictionaries())
        )).named(DataApiRequest.REQUEST_MAPPER_NAMESPACE).to(RequestMapper.class);

        binder.bind(requestMappers.getOrDefault(
                JobsApiRequest.REQUEST_MAPPER_NAMESPACE,
                new NoOpRequestMapper(loader.getDictionaries())
        )).named(JobsApiRequest.REQUEST_MAPPER_NAMESPACE).to(RequestMapper.class);
    }

    /**
     * Initializes the factory that builds druid queries.
     *
     * @return An isntance of the {@link DruidQueryBuilder}
     */
    protected Class<? extends DruidQueryBuilder> buildDruidQueryBuilder() {
        return DruidQueryBuilder.class;
    }

    /**
     * Initializes the class that parses responses from Druid.
     *
     * @return An instance of the {@link DruidResponseParser}
     */
    protected Class<? extends DruidResponseParser> buildDruidResponseParser() {
        return DruidResponseParser.class;
    }

    /**
     * Returns a clock for generating instants for timestamps.
     *
     * @return The clock with which to generate instants for timestamps, by default uses the system clock with the
     * system timezone
     */
    protected Clock getClock() {
        return Clock.system(
                ZoneId.of(SYSTEM_CONFIG.getStringProperty(
                        SYSTEM_CONFIG.getPackageVariableName(SYSTEM_CONFIG_TIMEZONE_KEY),
                        "UTC"
                ))
        );
    }

    /**
     * Initializes the factory that builds the asynchronous PreResponse processing workflow.
     *
     * @return An instance of the {@link DefaultAsynchronousWorkflowsBuilder}
     */
    protected Class<? extends AsynchronousWorkflowsBuilder> getAsynchronousProcessBuilder() {
        return DefaultAsynchronousWorkflowsBuilder.class;
    }

    /**
     * Initializes the service for building JobRows based on the information in a query.
     *
     * @return A factory for building JobRows, by default the factory builds a JobRow containing the fields
     * enumerated in {@link DefaultJobField}, using ids that are the concatenation of the user id with a uuid generated
     * by {@link UUID#randomUUID()}.
     */
    protected JobRowBuilder buildJobRowBuilder() {
        return new DefaultJobRowBuilder(
                jobMetadata -> jobMetadata.get(DefaultJobField.USER_ID) + UUID.randomUUID().toString()
        );
    }

    /**
     * Builds an instance of the interface to the storage system to use to store PreResponses of asynchronous
     * requests.
     *
     * @param resourceDictionaries  Hold the resource dictionaries
     *
     * @return An instance of the interface that allows Bard to talk to an arbitrary key-value store system for storing
     * PreResponses, by default this returns a {@code NoOpPreResponseStore}, which does nothing
     */
    protected PreResponseStore buildPreResponseStore(ResourceDictionaries resourceDictionaries) {
        return new NoOpPreResponseStore();
    }

    /**
     * Builds an instance of the interface to the storage system to use to store metadata about asynchronous
     * requests.
     *
     * @return An instance of the interface that allows Bard to talk to an arbitrary key-value store system for storing
     * job metadata, by default this returns a {@code NoOpApiJobStore}, which does nothing
     */
    protected ApiJobStore buildApiJobStore() {
        return new NoOpApiJobStore();
    }

    /**
     * Builds an instance of a BroadcastChannel that broadcasts the tickets of asynchronous jobs that have been
     * successfully stored in the PreResponseStore .
     *
     * @return A BroadcastChannel that allows Bard to talk to other Bard processes, by default returns the
     * {@link SimpleBroadcastChannel}, which uses a {@link PublishSubject} to allow cross-thread communication
     */
    protected BroadcastChannel<String> buildBroadcastChannel() {
        return new SimpleBroadcastChannel<>(PublishSubject.create());
    }

    /**
     * Initialize the PreResponse deserializer to deserialize the custom serialized PreResponse.
     *
     * @param dimensionDictionary  Contains all the dimension details
     * @param nonResponseContextMapper  ObjectMapper instance without any customization
     * @param responseContextMapper  ObjectMapper instance with customization to store/retrieve the value type
     * @param granularityParser  Granularity parser to get the time id
     *
     * @return instance of PreResponse deserializer
     */
    protected PreResponseDeserializer initializePreResponseDeserializer(
            DimensionDictionary dimensionDictionary,
            ObjectMapper nonResponseContextMapper,
            ObjectMapper responseContextMapper,
            GranularityParser granularityParser
    ) {
        return new PreResponseDeserializer(
                dimensionDictionary,
                nonResponseContextMapper,
                responseContextMapper,
                granularityParser
        );
    }

    /**
     * Initialize the service that decides which intervals in a request are volatile relative to a physical table.
     * By default, returns a `VolatileIntervalsService` that assumes none of the physical tables have any volatile
     * intervals.
     *
     * @return A service for determining which intervals in a request are volatile relative to a physical table
     */
    protected VolatileIntervalsService getVolatileIntervalsService() {
        return new DefaultingVolatileIntervalsService();
    }

    /**
     * Initialize the field converter.
     * By default it is SketchFieldConverter
     *
     * @return An instance of SketchFieldConverter
     */
    protected FieldConverters initializeSketchConverter() {
        return new ThetaSketchFieldConverter();
    }

    /**
     * Initialize the MetricsFilterSetBuilder. By default it is MetricsFilterSetBuilder
     *
     * @return An instance of MetricsFilterSetBuilder
     */
    protected MetricsFilterSetBuilder initializeMetricsFilterSetBuilder() {
        return new FilteredThetaSketchMetricsHelper(buildDruidFilterBuilder());
    }

    /**
     * Build an ObjectMappersSuite for everyone to use, since they are heavy-weight.
     *
     * @return The instance of ObjectMappersSuite
     */
    protected final ObjectMappersSuite getMappers() {
        if (objectMappers == null) {
            objectMappers = new ObjectMappersSuite();
        }
        return objectMappers;
    }

    /**
     * Return the instance of ObjectMapper defined in the current ObjectMappersSuite.
     *
     * @return The instance of ObjectMapper
     */
    protected final ObjectMapper getMapper() {
        return getMappers().getMapper();
    }

    /**
     * Register required core health checks.
     *
     * @param healthCheckRegistry  The health check registry
     * @param dimensionDictionary  The container with all dimensions
     */
    protected final void setupHealthChecks(
            HealthCheckRegistry healthCheckRegistry,
            DimensionDictionary dimensionDictionary
    ) {
        // Health checks are registered here since they should be configured at startup.
        HealthCheck allDimsLoaded = new AllDimensionsLoadedHealthCheck(dimensionDictionary);
        healthCheckRegistry.register(HEALTH_CHECK_NAME_DIMENSION, allDimsLoaded);
        healthCheckRegistry.register(HEALTH_CHECK_VERSION, new VersionHealthCheck());
    }

    /**
     * Register global gauges.
     */
    protected final void setupGauges() {
        // Gauges are registered here since they should be configured only once at startup.
        MetricRegistry metricRegistry = MetricRegistryFactory.getRegistry();
        Map<String, Metric> metrics = metricRegistry.getMetrics();

        if (!metrics.containsKey(METER_CACHE_HIT_RATIO)) {
            metricRegistry.register(
                    METER_CACHE_HIT_RATIO,
                    new RatioGauge() {
                        @Override
                        protected Ratio getRatio() {
                            return CACHE_REQUESTS.getCount() != 0
                                    ? Ratio.of(CACHE_HITS.getCount(), CACHE_REQUESTS.getCount())
                                    : Ratio.of(0, 1);
                        }
                    }
            );
        }

        if (!metrics.containsKey(METER_SPLITS_TOTAL_RATIO)) {
            metricRegistry.register(
                    METER_SPLITS_TOTAL_RATIO,
                    new RatioGauge() {
                        @Override
                        protected Ratio getRatio() {
                            return QUERY_REQUEST_TOTAL.getCount() != 0
                                    ? Ratio.of(SPLIT_QUERIES.getCount(), QUERY_REQUEST_TOTAL.getCount())
                                    : Ratio.of(0, 1);
                        }
                    }
            );
        }

        if (!metrics.containsKey(METER_SPLITS_RATIO)) {
            metricRegistry.register(
                    METER_SPLITS_RATIO,
                    new RatioGauge() {
                        @Override
                        protected Ratio getRatio() {
                            return SPLITS.getCount() != 0
                                    ? Ratio.of(SPLIT_QUERIES.getCount(), SPLITS.getCount())
                                    : Ratio.of(0, 1);
                        }
                    }
            );
        }

        if (!metrics.containsKey(JVM_UPTIME)) {
            metricRegistry.register(
                    JVM_UPTIME,
                    (Gauge<Long>) () -> ManagementFactory.getRuntimeMXBean().getUptime()
            );
        }
    }

    /**
     * Creates an object that constructs Druid dimension filters from Bard dimension filters.
     * <p>
     * Constructs a {@link DruidInFilterBuilder} by default.
     *
     * @return An object to build Druid filters from API filters
     */
    protected DruidFilterBuilder buildDruidFilterBuilder() {
        if (BardFeatureFlag.DEFAULT_IN_FILTER.isOn()) {
            return new DruidInFilterBuilder();
        } else {
            return new DruidOrFilterBuilder();
        }
    }

    /**
     * Creates an object that constructs Druid dimension filters from Bard dimension filters.
     * <p>
     * Constructs a {@link DruidInFilterBuilder} by default.
     *
     * @return An object to build Druid filters from API filters
     */
    protected DruidHavingBuilder buildDruidHavingBuilder() {
        return new DefaultDruidHavingBuilder();
    }

    /**
     * Creates an factory that constructs DataApiRequests
     * .
     * Constructs a {@link DefaultDataApiRequestFactory} by default.
     *
     * @return An object to build Druid filters from API filters
     */
    protected DataApiRequestFactory buildDataApiRequestFactory() {
        return new DefaultDataApiRequestFactory();
    }

    /**
     * Creates an object that generates map of Api Having from having string.
     * Constructs a {@link DefaultHavingApiGenerator} by default.
     * @param loader  Configuration loader that connects resource dictionaries with the loader.
     *
     * @return An object to generate having maps from having string.
     */
    protected HavingGenerator buildHavingGenerator(ConfigurationLoader loader) {
        return new PerRequestDictionaryHavingGenerator(new DefaultHavingApiGenerator(loader.getMetricDictionary()));
    }

    /**
     * Get the stored metadata service, if not exist yet, create the service and store it.
     *
     * @return A datasource metadata service
     */
    protected DataSourceMetadataService getDataSourceMetadataService() {
        if (Objects.isNull(dataSourceMetadataService)) {
            dataSourceMetadataService = new DataSourceMetadataService();
        }
        return dataSourceMetadataService;
    }

    /**
     * Build a QuerySigningService.
     *
     * @param physicalTableDictionary  the dictionary of physical tables
     * @param dataSourceMetadataService  A datasource metadata service
     *
     * @return A QuerySigningService
     */
    protected QuerySigningService<?> buildQuerySigningService(
            PhysicalTableDictionary physicalTableDictionary,
            DataSourceMetadataService dataSourceMetadataService
    ) {
        return new SegmentIntervalsHashIdGenerator(dataSourceMetadataService, buildSigningFunctions());
    }

    /**
     * Build a Map of Class to Function that should be used to get requestedIntervals from the DruidQuery.
     *
     * @return A Map that maps Class to a function that computes the requested intervals for a Druid query of
     * that particular Class
     */
    protected Map<Class, RequestedIntervalsFunction> buildSigningFunctions() {

        DefaultingDictionary<Class, RequestedIntervalsFunction> signingFunctions = new DefaultingDictionary<>(
                druidAggregationQuery -> new SimplifiedIntervalList(druidAggregationQuery.getIntervals())
        );

        signingFunctions.put(LookbackQuery.class, new LookbackQuery.LookbackQueryRequestedIntervalsFunction());

        return signingFunctions;
    }

    /**
     * Build a datasource metadata loader.
     *
     * @param webService  The web service used by the loader to query druid for segments availability.
     * @param physicalTableDictionary  The table to get the dimensions from.
     * @param metadataService  The service to be used to store the datasource metadata.
     * @param mapper  The object mapper to process the metadata json.
     *
     * @return A datasource metadata loader.
     */
    protected DataSourceMetadataLoadTask buildDataSourceMetadataLoader(
            DruidWebService webService,
            PhysicalTableDictionary physicalTableDictionary,
            DataSourceMetadataService metadataService,
            ObjectMapper mapper
    ) {
        return new DataSourceMetadataLoadTask(
                physicalTableDictionary,
                metadataService,
                webService,
                mapper
        );
    }

    /**
     * Builds a lookup metadata loader.
     *
     * @param webService  The web service used by the loader to query druid for lookup statuses.
     * @param dimensionDictionary  A {@link com.yahoo.bard.webservice.data.dimension.DimensionDictionary} that is used
     * to obtain a list of lookups in Fili.
     *
     * @return a lookup metadata loader
     */
    protected RegisteredLookupMetadataLoadTask buildLookupMetaDataLoader(
            DruidWebService webService,
            DimensionDictionary dimensionDictionary
    ) {
        return new RegisteredLookupMetadataLoadTask(webService, dimensionDictionary);
    }

    /**
     * Build a DimensionValueLoadTask.
     *
     * @param webService  The web service used by the loader to query dimension values
     * @param physicalTableDictionary  The table to update dimensions on
     * @param dimensionDictionary  The dimensions to update
     *
     * @return A DimensionValueLoadTask
     */
    protected DimensionValueLoadTask buildDruidDimensionsLoader(
            DruidWebService webService,
            PhysicalTableDictionary physicalTableDictionary,
            DimensionDictionary dimensionDictionary
    ) {
        DruidDimensionValueLoader druidDimensionRowProvider = new DruidDimensionValueLoader(
                physicalTableDictionary,
                dimensionDictionary,
                webService
        );
        return new DimensionValueLoadTask(Collections.singletonList(druidDimensionRowProvider));
    }

    /**
     * Schedule a datasource metadata loader and register its health check.
     *
     * @param healthCheckRegistry  The health check registry to register partial data health checks.
     * @param dataSourceMetadataLoader  The datasource metadata loader to use.
     */
    protected final void setupDataSourceMetaData(
            HealthCheckRegistry healthCheckRegistry,
            DataSourceMetadataLoadTask dataSourceMetadataLoader
    ) {
        scheduleLoader(dataSourceMetadataLoader);

        // Register Segment metadata loader health check
        HealthCheck dataSourceMetadataLoaderHealthCheck = new DataSourceMetadataLoaderHealthCheck(
                dataSourceMetadataLoader,
                SEG_LOADER_HC_LAST_RUN_PERIOD_MILLIS
        );
        healthCheckRegistry.register(HEALTH_CHECK_NAME_DATASOURCE_METADATA, dataSourceMetadataLoaderHealthCheck);
    }

    /**
     * Schedule a lookup metadata loader and register its health check.
     *
     * @param healthCheckRegistry  The health check registry to register lookup health checks.
     * @param registeredLookupMetadataLoadTask  The {@link RegisteredLookupMetadataLoadTask} to use.
     */
    protected final void setupLookUpMetadataLoader(
            HealthCheckRegistry healthCheckRegistry,
            RegisteredLookupMetadataLoadTask registeredLookupMetadataLoadTask
    ) {
        scheduleLoader(registeredLookupMetadataLoadTask);
        healthCheckRegistry.register(HEALTH_CHECK_NAME_LOOKUP_METADATA, new LookupHealthCheck(
                registeredLookupMetadataLoadTask));
    }

    /**
     * Schedule DimensionValueLoadTask and register its health check.
     *
     * @param healthCheckRegistry  The health check registry to register Dimension lookup health checks
     * @param dataDimensionLoader  The DruidDimensionLoader used for monitoring and health checks
     */
    protected final void setupDruidDimensionsLoader(
            HealthCheckRegistry healthCheckRegistry,
            DimensionValueLoadTask dataDimensionLoader
    ) {
        scheduleLoader(dataDimensionLoader);

        // Register DimensionValueLoadTask health check
        HealthCheck druidDimensionsLoaderHealthCheck = new DruidDimensionsLoaderHealthCheck(
                dataDimensionLoader,
                DRUID_DIM_LOADER_HC_LAST_RUN_PERIOD_MILLIS
        );
        healthCheckRegistry.register(HEALTH_CHECK_NAME_DRUID_DIM_LOADER, druidDimensionsLoaderHealthCheck);
    }

    /**
     * Return a workflow class to bind to RequestWorkflowProvider.
     *
     * @return a workflow class
     */
    protected Class<? extends RequestWorkflowProvider> getWorkflow() {
        return DruidWorkflow.class;
    }

    /**
     * Build a cache for data requests and matching responses.
     *
     * @return The cache instance
     */
    protected DataCache<?> buildCache() {
        if (CacheFeatureFlag.LOCAL_SIGNATURE.isOn()) {
            return buildLocalSignatureCache();
        } else if (CacheFeatureFlag.TTL.isOn()) {
            return buildTtlCache();
        } else if (CacheFeatureFlag.ETAG.isOn()) {
            return buildETagCahe();
        } else {
            // not used, but Jersey required a binding
            return new StubDataCache<>();
        }
    }

    /**
     * Builds and returns an instance of local signature cache.
     *
     * @return the instance of local signature cache
     */
    private DataCache<?> buildLocalSignatureCache() {
        if (BardFeatureFlag.DRUID_CACHE_V2.isSet()) {
            LOG.warn("Cache V2 feature flag is deprecated, " +
                    "use the new configuration parameter to set desired caching strategy"
            );
        }
        try {
            MemTupleDataCache<Long, String> cache = new MemTupleDataCache<>();
            LOG.info("MemcachedClient Version 2 started {}", cache);
            return cache;
        } catch (IOException e) {
            LOG.error("MemcachedClient Version 2 failed to start {}", e);
            throw new IllegalStateException(e);
        }
    }

    /**
     * Builds and returns an instance of TTL cache.
     *
     * @return the instance of TTL cache
     */
    private DataCache<?> buildTtlCache() {
        if (BardFeatureFlag.DRUID_CACHE.isSet()) {
            LOG.warn("Cache V1 feature flag is deprecated, " +
                    "use the new configuration parameter to set desired caching strategy"
            );
        }
        try {
            DataCache<String> cache = new HashDataCache<>(new MemDataCache<HashDataCache.Pair<String, String>>());
            LOG.info("MemcachedClient started {}", cache);
            return cache;
        } catch (IOException e) {
            LOG.error("MemcachedClient failed to start {}", e);
            throw new IllegalStateException(e);
        }
    }

    /**
     * Builds and returns an instance of eTag cache.
     *
     * @return the instance of eTag cache
     */
    private DataCache<?> buildETagCahe() {
        try {
            MemTupleDataCache<String, String> cache = new MemTupleDataCache<>();
            LOG.info("MemcachedClient Version 2 started {}", cache);
            return cache;
        } catch (IOException e) {
            LOG.error("MemcachedClient Version 2 failed to start {}", e);
            throw new IllegalStateException(e);
        }
    }

    /**
     * Asks for the valid feature flags that are expected to be defined in the system.
     * This method is also provided as an extension point for classes that need to add their own feature flags.
     * Such an extension is made easy when one of the final version of the method is used. For example:
     * <pre><code>
     * {@literal @}Override protected {@literal List<FeatureFlag>} collectFeatureFlags() {
     *      return collectFeatureFlags(AdditionalFeatureFlags.class);
     * }
     * </code></pre>
     *
     * @return The list of valid feature flags
     */
    protected List<FeatureFlag> collectFeatureFlags() {
        return Collections.emptyList();
    }

    /**
     * Given specific enumeration classes containing feature flags it returns a list with all the valid enumerations.
     *
     * @param enumerations  The enumeration classes that define feature flags
     *
     * @return The list of valid feature flags
     */
    @SafeVarargs
    protected final List<FeatureFlag> collectFeatureFlags(final Class<? extends FeatureFlag>... enumerations) {
        return collectFeatureFlags(Arrays.asList(enumerations));
    }

    /**
     * Given a list of enumeration classes containing feature flags it returns a list with all the valid enumerations.
     *
     * @param enumerations  A list with the enumeration classes that define feature flags
     *
     * @return A list with all the defined feature flags
     */
    protected final List<FeatureFlag> collectFeatureFlags(List<Class<? extends FeatureFlag>> enumerations) {
        return enumerations.stream()
                .map(Class::getEnumConstants)
                .flatMap(Arrays::stream)
                .collect(Collectors.toList());
    }

    /**
     * Get the application specific configuration loader, if not exist already, initialize with pluggable loaders.
     *
     * @return A configuration loader instance
     */
    protected final ConfigurationLoader getConfigurationLoader() {
        if (Objects.isNull(loader)) {
            loader = buildConfigurationLoader(getDimensionLoader(), getMetricLoader(), getTableLoader());
        }
        return loader;
    }

    /**
     * Extension point for building custom Configuration Loaders.
     *
     * @param dimensionLoader  A dimension loader
     * @param metricLoader  A metric loader
     * @param tableLoader  A table loader
     *
     * @return A configurationLoader instance
     */
    protected ConfigurationLoader buildConfigurationLoader(
            DimensionLoader dimensionLoader,
            MetricLoader metricLoader,
            TableLoader tableLoader
    ) {
        return new ConfigurationLoader(dimensionLoader, metricLoader, tableLoader);
    }

    /**
     * Create a Dimension Loader which will populate the dimensionDictionary.
     * The default Dimension Loader builds KeyValueStore Dimensions which are configured via the
     * {@link #getDimensionConfigurations()} method.
     *
     * @return a Dimension Loader instance
     */
    protected DimensionLoader getDimensionLoader() {
        return new TypeAwareDimensionLoader(getDimensionConfigurations());
    }

    /**
     * The Builder to be used to serialize a JobRow into the the job to be returned to the user.
     *
     * @return A DefaultJobPayloadBuilder
     */
    protected JobPayloadBuilder buildJobPayloadBuilder() {
        return new DefaultJobPayloadBuilder();
    }

    /**
     * Initializes the factory that builds HttpResponseMaker.
     *
     * @return an instance of the {@link HttpResponseMaker}
     */
    protected Class<? extends HttpResponseMaker> getHttpResponseMaker() {
        return HttpResponseMaker.class;
    }

    /**
     * Builds a response utils object with only CSV as a default always csv format.
     *
     * @return the response utils
     */
    protected ResponseUtils buildResponseUtils() {
        return new ResponseUtils();
    }

    /**
     * Builder for ResponseWriter, a serializer allowing customized response from Fili.
     *
     * @param mappers Shared instance of {@link com.fasterxml.jackson.databind.ObjectMapper}
     *
     * @return FiliResponseWriter
     */
    protected ResponseWriter buildResponseWriter(ObjectMappersSuite mappers) {
        return new FiliResponseWriter(
                new FiliResponseWriterSelector(
                        new CsvResponseWriter(mappers),
                        new JsonResponseWriter(mappers),
                        new JsonApiResponseWriter(mappers)
                )
        );
    }

    /**
     * Create a Metric Loader.
     * <p>
     * Metric loader populates the metricDictionary
     *
     * @return a metric loader instance
     */
    protected abstract MetricLoader getMetricLoader();

    /**
     * A set of all dimension configurations for this application.
     * <p>
     * These dimension configurations will be used to build the dimensions dictionary
     *
     * @return A set of configuration objects describing dimensions
     */
    protected abstract Set<DimensionConfig> getDimensionConfigurations();


    /**
     * Extension point for building a dictionary of base granularities for use in the system.
     *
     * @return A map of time grain api name (forced to lowercase) to time grain instances.
     */
    protected GranularityDictionary getGranularityDictionary() {
        return StandardGranularityParser.getDefaultGrainMap();
    }

    /**
     * Extension point for selecting GranularityParser implementations.
     *
     * @return A granularity parser instance
     */
    protected Class<? extends GranularityParser> getGranularityParser() {
        return StandardGranularityParser.class;
    }

    /**
     * Select a physical table resolver class.
     *
     * @return A table resolver implementation
     */
    protected Class<? extends PhysicalTableResolver> getPhysicalTableResolver() {
        return DefaultPhysicalTableResolver.class;
    }

    /**
     * Create a Table Loader.
     * <p>
     * Table loader populates the physicalTableDictionary and logicalTableDictionary
     *
     * @return A table loader instance
     */
    protected abstract TableLoader getTableLoader();

    /**
     * Get a map of named RequestMappers.
     * <p>
     * Create an empty map by default
     *
     * @param resourceDictionaries  the ResourceDictionaries used for constructing RequestMappers
     *
     * @return A map where the key is the name of the requestMapper for binding, and value is the requestMapper
     */
    protected @NotNull Map<String, RequestMapper> getRequestMappers(ResourceDictionaries resourceDictionaries) {
        return new HashMap<>(0);
    }

    /**
     * Create a DruidWebService.
     * <p>
     * Provided so subclasses can implement alternative druid web service implementations
     *
     * @param druidServiceConfig  Configuration for the Druid Service
     * @param mapper shared instance of {@link com.fasterxml.jackson.databind.ObjectMapper}
     *
     * @return A DruidWebService
     */
    protected DruidWebService buildDruidWebService(DruidServiceConfig druidServiceConfig, ObjectMapper mapper) {
        Supplier<Map<String, String>> supplier = buildDruidWebServiceHeaderSupplier();
        return DRUID_UNCOVERED_INTERVAL_LIMIT > 0
                ? new AsyncDruidWebServiceImpl(
                        druidServiceConfig,
                        mapper,
                        supplier,
                        new HeaderNestingJsonBuilderStrategy(
                                AsyncDruidWebServiceImpl.DEFAULT_JSON_NODE_BUILDER_STRATEGY
                        )
                )
                : new AsyncDruidWebServiceImpl(druidServiceConfig, mapper, supplier);
    }

    /**
     * Build the Supplier for Druid data request headers.
     *
     * @return The Druid data request header Supplier.
     */
    protected Supplier<Map<String, String>> buildDruidWebServiceHeaderSupplier() {
        Supplier<Map<String, String>> supplier = HashMap::new;
        String customSupplierClassString = SYSTEM_CONFIG.getStringProperty(DRUID_HEADER_SUPPLIER_CLASS, null);
        if (customSupplierClassString != null && !customSupplierClassString.equals("")) {
            try {
                @SuppressWarnings("unchecked")
                Class<? extends Supplier<Map<String, String>>> c = (Class) Class
                        .forName(customSupplierClassString).asSubclass(Supplier.class);
                supplier = c.getConstructor().newInstance();
            } catch (Exception e) {
                LOG.error(
                        "Unable to load the Druid query header supplier, className: {}, exception: {}",
                        customSupplierClassString,
                        e
                );
                throw new IllegalStateException(e);
            }
        }
        return supplier;
    }

    /**
     * Create a DruidWebService for the UI connection.
     * <p>
     * Provided so subclasses can implement alternative druid web service implementations for the UI connection
     *
     * @param mapper shared instance of {@link com.fasterxml.jackson.databind.ObjectMapper}
     *
     * @return A DruidWebService
     */
    protected DruidWebService buildDruidWebService(ObjectMapper mapper) {
        return buildDruidWebService(DruidClientConfigHelper.getServiceConfig(), mapper);
    }

    /**
     * Create a DruidWebService for metadata.
     *
     * @param mapper shared instance of {@link com.fasterxml.jackson.databind.ObjectMapper}
     *
     * @return A DruidWebService
     */
    protected DruidWebService buildMetadataDruidWebService(ObjectMapper mapper) {
        return buildDruidWebService(DruidClientConfigHelper.getMetadataServiceConfig(), mapper);
    }

    /**
     * Create a ResponseFormatResolver for Servlet objects.
     * <p>
     * Currently default types are json, jsonapi and csv types.
     *
     * @return A ResponseFormatResolver
     */
    protected ResponseFormatResolver buildResponseFormatResolver() {
        return new DefaultResponseFormatResolver();
    }

    /**
     * Returns the class to bind to {@link ResponseProcessorFactory}.
     * <p>
     * The ResponseProcessorFactory allows us to inject a custom {@link
     * com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor} despite the fact that these processors depend
     * on objects that are built uniquely for each request.
     *
     * @return A class that implements {@link ResponseProcessorFactory}.
     */
    protected Class<? extends ResponseProcessorFactory> buildResponseProcessorFactory() {
        return ResultSetResponseProcessorFactory.class;
    }

    /**
     * Creates a new RateLimiter for the RateLimitFilter.
     *
     * @return a RateLimiter implementation
     */
    protected RateLimiter buildRateLimiter() {
        return new DefaultRateLimiter();
    }

    @Override
    public void afterRegistration(ResourceConfig resourceConfig) {
        // NoOp by default
    }

    /**
     * Allows additional app-specific binding.
     *
     * @param abstractBinder  Binder to use for binding
     */
    protected void afterBinding(AbstractBinder abstractBinder) {
        // NoOp by default
    }

    /**
     * Schedule a loadTask task.
     *
     * @param loadTask  The loadTask task to run.
     */
    protected void scheduleLoader(LoadTask<?> loadTask) {
        loadTask.setFuture(
                loadTask.isPeriodic ?
                        loaderScheduler.scheduleAtFixedRate(
                                loadTask,
                                loadTask.getDefinedDelay(),
                                loadTask.getDefinedPeriod(),
                                TimeUnit.MILLISECONDS
                        ) :
                        loaderScheduler.schedule(loadTask, loadTask.getDefinedDelay(), TimeUnit.MILLISECONDS)
        );
    }

    /**
     * Shutdown the scheduler for loader tasks.
     */
    protected void shutdownLoaderScheduler() {
        loaderScheduler.shutdownNow();
    }

    /**
     * Make sure the scheduler for loader tasks shuts down when the resources of this class are released.
     *
     * @throws Throwable  An exception raised by this method.
     */
    @Override
    protected void finalize() throws Throwable {
        try {
            shutdownLoaderScheduler();
        } finally {
            super.finalize();
        }
    }
}
