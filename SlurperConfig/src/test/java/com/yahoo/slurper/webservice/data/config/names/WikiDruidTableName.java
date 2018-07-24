package com.yahoo.slurper.webservice.data.config.names;

import com.yahoo.bard.webservice.data.config.names.TableName;

import java.util.Locale;

public enum WikiDruidTableName implements TableName {
    WIKITICKER;

    private final String lowerCaseName;

    /**
     * Create a table name instance.
     */
    WikiDruidTableName() {
        this.lowerCaseName = name().toLowerCase(Locale.ENGLISH);
    }

    /**
     * View this table name as a string.
     *
     * @return The table name as a string
     */
    public String asName() {
        return lowerCaseName;
    }
}
