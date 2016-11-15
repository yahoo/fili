// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.data.config.metric.makers.ConstantMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.CountMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.DoubleMaxMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.DoubleMinMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.DoubleSumMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.LongMaxMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.LongMinMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.LongSumMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.RowNumMaker;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Dictionary of MetricMakers
 *
 * In The Future, this could be an interface that can easily be changed
 *
 * For now, a bunch of yucky reflection code lives in here.
 */
public class MakerDictionary extends LinkedHashMap<String, MetricMaker> {
    private static final long serialVersionUID = 4296501494537995054L;

    // The package name
    private static final String MAKER_PACKAGE_NAME = MetricMaker.class.getPackage().getName();

    // List of 'built-in' metric makers
    // This used to use Reflections, but that seems like overkill
    private static final Set<Class<? extends MetricMaker>> BUILTIN_METRIC_MAKERS = Collections.unmodifiableSet(
            new HashSet<>(
                    Arrays.asList(
                            ConstantMaker.class,
                            CountMaker.class,
                            DoubleMaxMaker.class,
                            DoubleMinMaker.class,
                            DoubleSumMaker.class,
                            LongMaxMaker.class,
                            LongMinMaker.class,
                            LongSumMaker.class,
                            RowNumMaker.class

                            // These (and a few others) are built in but require some arguments
                            // So, you'll have to list them as custom makers with their parameters
                            // AggregationAverageMaker.class,
                            // CardinalityMaker.class,
                    )
            )
    );

    // Cache of default metric makers
    private static MakerDictionary defaultMakers;

    /**
     * Get a map of name -&gt; makers for the default (built-in) MetricMakers.
     *
     * Makers included by default:
     *
     * 1. Are in the same package as MetricMaker
     * 2. Have a constructor that takes a single MetricDictionary
     *
     * @param dict metric dictionary to use for built-in makers
     * @return the dictionary of MetricMakers
     */
    public static MakerDictionary getDefaultMakers(MetricDictionary dict) {
        if (defaultMakers == null) {
            defaultMakers = new MakerDictionary();
            for (Class<? extends MetricMaker> cls : BUILTIN_METRIC_MAKERS) {
                // LongSumMaker => longSum
                String makerName = lowerFirst(cls.getSimpleName()).replaceFirst("Maker", "");
                MetricMaker actualMaker = instantiateObjectFromArgs(cls, dict);
                if (actualMaker != null) {
                    defaultMakers.put(makerName, actualMaker);
                } else {
                    throw new RuntimeException("Unable to instantiate built-in metric maker " + cls);
                }
            }
        }

        return defaultMakers;
    }

    /**
     * Initialize the metric makers, custom and built-in.
     *
     * @param dict MetricDictionary to use for every maker
     * @param mDict Dictionary of function name to MetricMaker
     * @param makers user-defined config listing custom MetricMakers
     */
    public static void loadMetricMakers(
            MetricDictionary dict,
            MakerDictionary mDict,
            Map<String, MakerConfiguration> makers
    ) {
        loadMetricMakers(dict, mDict, makers, false);
    }

    /**
     * Initialize the metric makers, custom and optionally built-in.
     *
     * Eventually, 'skipDefault' will be an option in the YAML config.
     *
     * @param dict MetricDictionary to use for every maker
     * @param mDict Dictionary of function name to MetricMaker
     * @param makers user-defined config listing custom MetricMakers
     * @param skipDefault true to skip loading built-in metrics
     */
    public static void loadMetricMakers(
            MetricDictionary dict,
            MakerDictionary mDict,
            Map<String, MakerConfiguration> makers,
            boolean skipDefault
    ) {

        if (!skipDefault) {
            mDict.putAll(getDefaultMakers(dict));
        }

        if (makers != null) {
            Map<String, MetricMaker> customMakers = makers.entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            v -> buildCustomMaker(v.getKey(),
                                    v.getValue().getClassName(),
                                    v.getValue().getArguments(),
                                    dict
                            )
                    ));

            mDict.putAll(customMakers);
        }
    }

    /**
     * Find the full package + class name for a maker.
     *
     * The class name chosen for the custom maker is one of:
     *
     * 1. The full package + class name specified in the className field
     * 2. The name specified in the className field inside the MetricMaker package
     * 3. If no className given, the Maker name specified (longSum), turned into a Maker class name
     *    (LongSumMaker), inside the MetricMaker package
     *
     * @param suppliedMakerName The maker name the user specified
     * @param suppliedClassName The class name the user provided
     * @return The resolved class name
     */
    protected static String getClassName(String suppliedMakerName, String suppliedClassName) {
        if (suppliedClassName == null || suppliedClassName.isEmpty()) {
            return MAKER_PACKAGE_NAME + "." + suppliedMakerName.substring(0, 1)
                    .toUpperCase(Locale.ENGLISH) + suppliedMakerName.substring(1) + "Maker";
        } else if (suppliedClassName.contains(".")) {
            return suppliedClassName;
        } else {
            return MAKER_PACKAGE_NAME + "." + suppliedClassName;
        }
    }

    /**
     * Construct a custom MetricMaker given its class name and arguments.
     *
     * @param name The pretty name ("longSum")
     * @param className The class name for the maker
     * @param args Arguments to the maker constructor
     * @param dict Metric dictionary
     * @return A metric maker
     */
    protected static MetricMaker buildCustomMaker(String name, String className, Object[] args, MetricDictionary dict) {
        Class cls;
        try {
            cls = Class.forName(getClassName(name, className));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Error loading metric maker `" + name + "`: Could not find `" + className + "`" +
                    " in classpath");
        }

        if (!MetricMaker.class.isAssignableFrom(cls)) {
            throw new RuntimeException("Error loading metric maker `" + name + "`: Could not instantiate " +
                    className + "; seems to not inherit from MetricMaker");
        } else {

            Object[] instArgs = new Object[args.length + 1];
            instArgs[0] = dict;
            System.arraycopy(args, 0, instArgs, 1, args.length);

            MetricMaker m = (MetricMaker) instantiateObjectFromArgs(cls, instArgs);

            if (m == null) {
                throw new RuntimeException("Could not instantiate " + className + "; no suitable constructor found");
            }

            return m;
        }
    }

    /**
     * Instantiate an object from a class name and parameters.
     *
     * @param type the class to instantiate
     * @param args the constructor arguments
     * @param <T> the type of object to return
     * @return an instantiated object, or null
     */
    public static <T> T instantiateObjectFromArgs(Class<? extends T> type, Object... args) {

        if (type == null || Modifier.isAbstract(type.getModifiers()) || !Modifier.isPublic(type.getModifiers()) ||
                args == null) {
            return null;
        }

        // Find a valid constructor and build an instance
        Constructor<?>[] constructors = type.getConstructors();
        for (Constructor<?> ctor : constructors) {
            if (ctor.getParameterCount() == args.length) {
                int argc = 0;
                boolean success = true;

                for (Class<?> pType : ctor.getParameterTypes()) {
                    Object arg = args[argc];

                    // Ugh. This is not an exhaustive list;
                    // the problem is that int.class is not isAssignableFrom Integer.class.
                    if ((pType == int.class && arg.getClass() == Integer.class) || (pType == Integer.class && arg
                            .getClass() == int.class)) {
                        // OK...
                    } else if ((pType == long.class && arg.getClass() == Long.class) || (pType == Long.class && arg
                            .getClass() == long.class)) {
                        // OK...

                        // Note that this allows null arguments; that may not be what we want.
                    } else if (arg != null && !pType.isAssignableFrom(arg.getClass())) {
                        success = false;
                        break;
                    }
                    argc += 1;
                }

                if (success) {
                    try {
                        return (T) ctor.newInstance(args);
                    } catch (Exception e) {
                        throw new RuntimeException("Could not construct metricMaker " + type, e);
                    }
                }
            }
        }

        return null;
    }

    /**
     * Make the first letter of the input string lowercase.
     *
     * @param input any string
     * @return a new copy of the input string with the first letter lower-cased
     */
    protected static String lowerFirst(String input) {
        if (input == null || input.length() == 0) {
            return input;
        } else {
            return input.substring(0, 1).toLowerCase(Locale.ENGLISH) + input.substring(1);
        }
    }
}
