/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.geotools.dataflownodes;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataProcessor;
import com.arjuna.dbplugins.geotools.connectors.SimpleDataConsumer;
import com.arjuna.dbplugins.geotools.connectors.SimpleDataProvider;

public class ShapeFileConverterDataProcessor implements DataProcessor
{
    private static final Logger logger = Logger.getLogger(ShapeFileConverterDataProcessor.class.getName());

    public ShapeFileConverterDataProcessor(String name, Map<String, String> properties)
    {
        logger.log(Level.INFO, "SimpleDataProcessor: " + name + ", " + properties);

        _name       = name;
        _properties = properties;

        _dataConsumer = new SimpleDataConsumer<File>(this, MethodUtil.getMethod(ShapeFileConverterDataProcessor.class, "convert", File.class));
        _dataProvider = new SimpleDataProvider<String>(this);
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public Map<String, String> getProperties()
    {
        return Collections.unmodifiableMap(_properties);
    }

    public void convert(File shapeFile)
    {
        logger.log(Level.INFO, "ShapeFileConverterDataProcessor.convert: " + shapeFile.getName());

        _dataProvider.produce("[" + shapeFile.getName() + "]");
    }

    @Override
    public Collection<Class<?>> getDataConsumerDataClasses()
    {
        Set<Class<?>> dataConsumerDataClasses = new HashSet<Class<?>>();

        dataConsumerDataClasses.add(File.class);
        
        return dataConsumerDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataConsumer<T> getDataConsumer(Class<T> dataClass)
    {
        if (dataClass == File.class)
            return (DataConsumer<T>) _dataConsumer;
        else
            return null;
    }

    @Override
    public Collection<Class<?>> getDataProviderDataClasses()
    {
        Set<Class<?>> dataProviderDataClasses = new HashSet<Class<?>>();

        dataProviderDataClasses.add(String.class);
        
        return dataProviderDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataProvider<T> getDataProvider(Class<T> dataClass)
    {
        if (dataClass == String.class)
            return (DataProvider<T>) _dataProvider;
        else
            return null;
    }

    private String               _name;
    private Map<String, String>  _properties;
    private DataConsumer<File>   _dataConsumer;
    private DataProvider<String> _dataProvider;
}
