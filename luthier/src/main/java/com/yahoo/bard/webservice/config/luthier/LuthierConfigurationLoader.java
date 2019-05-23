package com.yahoo.bard.webservice.config.luthier;

import com.yahoo.bard.webservice.data.config.ConfigurationLoader;
import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionLoader;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;

// TODO Make Configuration Loader an interface
public class LuthierConfigurationLoader implements ConfigurationLoader {

    ResourceDictionaries resourceDictionaries;

    public LuthierConfigurationLoader(ResourceDictionaries resourceDictionaries) {
        this.resourceDictionaries = resourceDictionaries;
    }

    @Override
    public void load() {
        registerFactories();
    }

    /**
     * The method customers should overload to register factories for custom
     * configuration types. 
     */
    publid void registerFactories(LuthierIndustrialPark industrialPark) {
    }

    @Override
    public DimensionDictionary getDimensionDictionary() {
        return resourceDictionaries.getDimensionDictionary();
    }

    @Override
    public MetricDictionary getMetricDictionary() {
        return resourceDictionaries.getMetricDictionary();
    }

    @Override
    public LogicalTableDictionary getLogicalTableDictionary() {
        return resourceDictionaries.getLogicalDictionary();
    }

    @Override
    public PhysicalTableDictionary getPhysicalTableDictionary() {
        return getPhysicalTableDictionary();
    }

    @Override
    public ResourceDictionaries getDictionaries() {
        return resourceDictionaries;
    }
}
