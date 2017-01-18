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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Build MetricMakers on-the-fly.
 *
 * Used so that the MetricDictionary can be supplied at metric build time and not before.
 */
public class MakerBuilder {

    // The package name
    private static final String MAKER_PACKAGE_NAME = MetricMaker.class.getPackage().getName();

    private static final Logger LOG = LoggerFactory.getLogger(MakerBuilder.class);

    private Map<String, MakerConstructor> availableMakerConstructors = new HashMap<>();

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
                            // So, you'll have to list them as custom makers with their parameters.
                            // AggregationAverageMaker.class,
                            // CardinalityMaker.class,
                    )
            )
    );

    /**
     * Small utility class for constructing a single MetricMaker.
     */
    public static class MakerConstructor {
        private final Class<? extends MetricMaker> cls;
        private final Object[] args;

        /**
         * Constructor for MakerConstructor.
         *
         * @param cls  specific class of MetricMaker to construct
         * @param args  arguments to the MetricMaker's constructor
         */
        public MakerConstructor(Class<? extends MetricMaker> cls, Object[] args) {
            this.cls = cls;
            this.args = args;
        }

        /**
         * Construct a new MetricMaker.
         *
         * Uses reflection to construct an object out of `cls` and `args`,
         * but always inserts `dictionary` as the first parameter.
         *
         * @param dictionary  metric dictionary
         *
         * @return a MetricMaker
         */
        public MetricMaker construct(MetricDictionary dictionary) {
            Object[] instArgs;
            if (args == null) {
                instArgs = new Object[]{dictionary};
            } else {
                instArgs = new Object[args.length + 1];
                instArgs[0] = dictionary;
                System.arraycopy(args, 0, instArgs, 1, args.length);
            }

            MetricMaker maker = instantiateObjectFromArgs(cls, instArgs);

            if (maker == null) {
                throw new ConfigurationError("Could not instantiate " + cls + "; no suitable constructor found");
            }

            return maker;
        }

        @Override
        public int hashCode() {
            // Sufficient for our use case to just use 'cls'
            return Objects.hashCode(this.cls);
        }


        @Override
        public boolean equals(Object other) {
            if (other == null || !(other instanceof MakerConstructor)) {
                return false;
            }

            MakerConstructor obj = (MakerConstructor) other;

            return Objects.equals(this.cls, obj.cls) &&
                    Arrays.equals(this.args, obj.args);
        }
    }

    /**
     * Construct a new MakerBuilder.
     *
     * This class builds a new MetricMaker on-the-fly when you ask for it (nicely) by name.
     *
     * @param configuredMakers  Metric maker configuration
     */
    public MakerBuilder(List<MakerConfiguration> configuredMakers) {

        // Add any default/builtin metric makers
        for (Class<? extends MetricMaker> cls : BUILTIN_METRIC_MAKERS) {
            String makerName = lowerFirst(cls.getSimpleName()).replaceFirst("Maker", "");
            availableMakerConstructors.put(makerName, new MakerConstructor(cls, null));
        }

        // Add any custom metric makers
        if (configuredMakers != null) {
            for (MakerConfiguration makerConfig: configuredMakers) {
                String name = makerConfig.getName();
                if (availableMakerConstructors.containsKey(name)) {
                    LOG.warn("Overriding previously-defined maker: {}", name);
                }
                Class<? extends MetricMaker> makerCls = buildMakerClass(name, makerConfig.getClassName());
                availableMakerConstructors.put(name, new MakerConstructor(makerCls, makerConfig.getArguments()));
            }
        }
    }

    /**
     * Build the metric maker for the given name and with the given metric dictionary.
     *
     * @param name  pretty maker name, e.g. longSum
     * @param dictionary  metric dictionary
     *
     * @return the metric maker
     */
    public MetricMaker build(String name, MetricDictionary dictionary) {
        if (!availableMakerConstructors.containsKey(name)) {
            throw new ConfigurationError("Asked for maker " + name + " but do not know how to construct it.");
        }
        MakerConstructor reflector = availableMakerConstructors.get(name);

        return reflector.construct(dictionary);
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
     * @param suppliedMakerName  The maker name the user specified
     * @param suppliedClassName  The class name the user provided
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
     * Attempt to find the Class corresponding to the given name or class name.
     *
     * @param name  the pretty maker name, e.g. longSum
     * @param className  the class name; optional if built-in
     *
     * @return the class corresponding to the given name or class name
     */
    protected static Class<? extends MetricMaker> buildMakerClass(String name, String className) {

        Class cls;
        try {
            cls = Class.forName(getClassName(name, className));
        } catch (ClassNotFoundException e) {
            throw new ConfigurationError("Error loading metric maker `" + name + "`: Could not find `" + className +
                    "`" +
                    " in classpath");
        }

        // I don't think you can do this without an unchecked cast.
        if (!MetricMaker.class.isAssignableFrom(cls)) {
            throw new ConfigurationError("Error loading metric maker `" + name + "`: Could not instantiate " +
                    className + "; seems to not inherit from MetricMaker");
        } else {
            return (Class<? extends MetricMaker>) cls;
        }
    }

    /**
     * Instantiate an object from a class name and parameters.
     *
     * @param type  the class to instantiate
     * @param args  the constructor arguments
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

                    // This allows null arguments; that may not be what we want, but OK for now
                    if (arg != null && !isUnboxingCase(pType, arg.getClass()) &&
                            !pType.isAssignableFrom(arg.getClass())) {
                        success = false;
                        break;
                    }
                    argc += 1;
                }

                if (success) {
                    try {
                        return (T) ctor.newInstance(args);
                    } catch (Exception e) {
                        throw new ConfigurationError("Could not construct metricMaker " + type, e);
                    }
                }
            }
        }

        return null;
    }

    /**
     * Return true if the two classes are boxed/unboxed versions of each other.
     *
     * @param pClass  parameter class
     * @param argClass  argument class
     * @return true if they match, false otherwise
     */
    private static boolean isUnboxingCase(Class<?> pClass, Class<?> argClass) {
        // This is not an exhaustive list; the problem is that int.class is not isAssignableFrom Integer.class
        // same is true for all primitive types
        if (pClass == int.class && argClass == Integer.class) {
            return true;
        } else if (pClass == Integer.class && argClass == int.class) {
            return true;
        } else if (pClass == long.class && argClass == Long.class) {
            return true;
        } else if (pClass == Long.class && argClass == long.class) {
            return true;
        }

        return false;
    }

    /**
     * Make the first letter of the input string lowercase.
     *
     * @param input  any string
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
