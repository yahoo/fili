package com.yahoo.wiki.webservice.data.config.table;

import com.yahoo.bard.webservice.util.EnumUtils;

/**
 * Types of {@link com.yahoo.bard.webservice.table.PhysicalTable} which are
 * supported by {@link GenericTableLoader}.
 */
public enum PhysicalTableType {
    CONCRETE,
    PERMISSIVE;

    private final String type;

    PhysicalTableType() {
        type = EnumUtils.camelCase(name());
    }

    public static PhysicalTableType fromType(String type) {
        for(PhysicalTableType tableType : values()) {
            if(tableType.type.equals(type)) {
                return tableType;
            }
        }
        return null;
    }
}
