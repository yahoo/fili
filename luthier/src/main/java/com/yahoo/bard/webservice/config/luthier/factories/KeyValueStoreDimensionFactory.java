package com.yahoo.bard.webservice.config.luthier.factories;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.bard.webservice.config.luthier.Factory;
import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy;
import org.joda.time.DateTime;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class KeyValueStoreDimensionFactory implements Factory<Dimension> {

    @Override
    public Dimension build(String name, ObjectNode configTable, LuthierIndustrialPark resourceFactories) {
        return new Dimension() {
            @Override
            public void setLastUpdated(DateTime lastUpdated) {

            }

            @Override
            public String getApiName() {
                return name;
            }

            @Override
            public String getDescription() {
                return null;
            }

            @Override
            public DateTime getLastUpdated() {
                return null;
            }

            @Override
            public LinkedHashSet<DimensionField> getDimensionFields() {
                return null;
            }

            @Override
            public LinkedHashSet<DimensionField> getDefaultDimensionFields() {
                return null;
            }

            @Override
            public DimensionField getFieldByName(String name) {
                return null;
            }

            @Override
            public SearchProvider getSearchProvider() {
                return null;
            }

            @Override
            public void addDimensionRow(DimensionRow dimensionRow) {

            }

            @Override
            public void addAllDimensionRows(Set<DimensionRow> dimensionRows) {

            }

            @Override
            public DimensionRow findDimensionRowByKeyValue(String value) {
                return null;
            }

            @Override
            public DimensionField getKey() {
                return null;
            }

            @Override
            public DimensionRow parseDimensionRow(Map<String, String> fieldNameValueMap) {
                return null;
            }

            @Override
            public DimensionRow createEmptyDimensionRow(String keyFieldValue) {
                return null;
            }

            @Override
            public String getCategory() {
                return null;
            }

            @Override
            public String getLongName() {
                return null;
            }

            @Override
            public StorageStrategy getStorageStrategy() {
                return null;
            }

            @Override
            public int getCardinality() {
                return 0;
            }

            @Override
            public boolean isAggregatable() {
                return false;
            }
        };
    }
}
