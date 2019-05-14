package com.yahoo.bard.webservice.config.luthier;

import com.yahoo.bard.webservice.data.config.ConfigurationLoader;
import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionLoader;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;

// TODO Make Configuration Loader an interface
public class LuthierConfigurationLoader extends ConfigurationLoader {

    ResourceDictionaries resourceDictionaries;

    public LuthierConfigurationLoader(
            final DimensionLoader dimensionLoader,
            final MetricLoader metricLoader,
            final TableLoader tableLoader,
            final ResourceDictionaries resourceDictionaries
    ) {
        super(dimensionLoader, metricLoader, tableLoader);
        this.resourceDictionaries = resourceDictionaries;
    }


}
