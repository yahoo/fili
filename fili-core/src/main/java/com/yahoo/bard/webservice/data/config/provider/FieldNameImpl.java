package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.data.config.names.FieldName;

import java.util.Objects;

/**
 * A FieldName.
 */
public class FieldNameImpl implements FieldName {
    protected final String name;

    /**
     * Construct a new name.
     *
     * @param name  The field name
     */
    public FieldNameImpl(String name) {
        this.name = name;
    }

    @Override
    public String asName() {
        return name;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof FieldNameImpl)) {
            return false;
        } else {
            return Objects.equals(this.name, ((FieldNameImpl) other).name);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.name);
    }
}
