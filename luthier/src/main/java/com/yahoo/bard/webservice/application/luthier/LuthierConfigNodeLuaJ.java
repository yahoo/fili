// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application.luthier;

import com.yahoo.bard.webservice.exceptions.LuthierFactoryException;

import org.apache.commons.lang3.tuple.Pair;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * An implementation of LuthierConfigNode backed by LuaJ.
 */
public class LuthierConfigNodeLuaJ implements LuthierConfigNode {

    private static final Logger LOG = LoggerFactory.getLogger(LuthierConfigNodeLuaJ.class);

    private final LuaValue configuration;
    /**
     * Constructor.
     *
     * @param configuration The LuaJ LuaValue this class wraps
     */
    public LuthierConfigNodeLuaJ(LuaValue configuration) {
        this.configuration = configuration;
    }

    /**
     * Returns the LuaValue hiding inside this node.
     *
     * Allows customers to get at the "guts" so to speak, in case they
     * need/want to do something that this interface doesn't enable.
     *
     * @return LuaValue the underlying Lua value.
     */
    public LuaValue getLuaValue() {
        return configuration;
    }

    @Override
    public LuthierConfigNode get(String key) {
        LuaValue value = configuration.get(key);
        return value == null || value.isnil() ? null : new LuthierConfigNodeLuaJ(value);
    }

    @Override
    public boolean has(String fieldName) {
        return !configuration.get(fieldName).isnil();
    }

    /**
     * Returns an iterator over the key-value pairs in the lua table wrapped
     * in this node.
     *
     * If this node does not wrap a table, the iterator is empty.
     *
     * @return An iterator over the key-value pairs in the table wrapped in
     * this node, or an empty iterator if the node is not a table
     */
    public Iterator<Pair<LuaValue, LuaValue>> keyValuePairIterator() {
        return new Iterator<Pair<LuaValue, LuaValue>>() {
            private LuaValue key = LuaValue.NIL;

            @Override
            public boolean hasNext() {
                try {
                    return !configuration.next(key).arg1().isnil();
                } catch (LuaError e) {
                    LOG.warn("Attempted to iterate over a non-table Lua value.", e);
                    return false;
                }
            }

            @Override
            public Pair<LuaValue, LuaValue> next() {
                try {
                    Varargs keyvaluepair = configuration.next(key);
                    key = keyvaluepair.arg1();
                    Pair<LuaValue, LuaValue> pair = Pair.of(
                            key,
                            keyvaluepair.arg(2)
                    );
                    return pair;
                } catch (LuaError e) {
                    LOG.warn("Attempted to iterate over a non-table Lua value.", e);
                    return null;
                }
            }
        };
    }

    @Override
    public Iterator<LuthierConfigNode> iterator() {

        return new Iterator<LuthierConfigNode>() {

            private Iterator<Pair<LuaValue, LuaValue>> iterator = keyValuePairIterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public LuthierConfigNode next() {
                Pair<LuaValue, LuaValue> keyValuePair = iterator.next();
                return keyValuePair == null ? null : new LuthierConfigNodeLuaJ(keyValuePair.getRight());
            }
        };
    }

    @Override
    public Iterator<String> fieldNames() {
        return new Iterator<String>() {
            private final Iterator<Pair<LuaValue, LuaValue>> iterator = keyValuePairIterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public String next() {
                Pair<LuaValue, LuaValue> keyValuePair = iterator.next();
                return keyValuePair == null ? null : keyValuePair.getLeft().tojstring();
            }
        };
    }

    @Override
    public String textValue() {
        if (configuration.isstring() && !configuration.isnumber()) {
            return configuration.tojstring();
        }
        throw new LuthierFactoryException(
                "Attempted to convert a non-string value to a String: '" + configuration + "'"
        );
    }

    @Override
    public boolean booleanValue() {
        if (configuration.isboolean()) {
            return configuration.toboolean();
        }
        throw new LuthierFactoryException(
                "Attempted to convert a non-boolean value to a boolean: '" + configuration + "'"
        );
    }

    @Override
    public long longValue() {
        if (configuration.islong()) {
            return configuration.tolong();
        }
        throw new LuthierFactoryException(
                "Attempted to convert a non-long value to a long: '" + configuration + "'"
        );
    }

    @Override
    public int intValue() {
        if (configuration.isint()) {
            return configuration.toint();
        }
        throw new LuthierFactoryException(
                "Attempted to convert a non-long value to a long: '" + configuration + "'"
        );
    }

    /**
     * This is a very quick and dirty toString that fully prints the key-value pairs of a table if the
     * node wraps a table, and the LuaValue jstring for all other types.
     * <p>
     * Be warned, this toString will recurse infinitely if the table is recursive (i.e. has a reference to
     * itself as a key or value). However, since this system is used only for configuration, that almost
     * certainly won't happen. If someone wishes to plug the edge case, they are of course free to.
     */
    @Override
    public String toString() {
        if (configuration.istable()) {
            StringBuffer string = new StringBuffer("{");
            Iterator<Pair<LuaValue, LuaValue>> keyValuePairs = keyValuePairIterator();
            while (keyValuePairs.hasNext()) {
                Pair<LuaValue, LuaValue> keyValuePair = keyValuePairs.next();
                string.append(String.format("%s=%s", keyValuePair.getLeft(), keyValuePair.getRight()));
            }
            string.append("}");
            return string.toString();
        }
        return configuration.tojstring();
   }
}
