/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.geotools.dataflownodes;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.geometry.primitive.Point;

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

    public void convert(File shapefileFile)
    {
        logger.log(Level.INFO, "ShapeFileConverterDataProcessor.convert: " + shapefileFile.getName());

        try
        {
            FileDataStore           fileDataStore     = FileDataStoreFinder.getDataStore(shapefileFile);
            SimpleFeatureSource     featureSource     = fileDataStore.getFeatureSource();
            SimpleFeatureCollection featureCollection = featureSource.getFeatures();
            SimpleFeatureIterator   featureIterator   = featureCollection.features();
            SimpleFeatureType       schema            = featureCollection.getSchema();

            List<AttributeDescriptor> attributeDescriptors = schema.getAttributeDescriptors();
            while (featureIterator.hasNext())
            {
                SimpleFeature feature = featureIterator.next();

                boolean      firstAttribute = true;
                StringBuffer line           = new StringBuffer();
                for (AttributeDescriptor attributeDescriptor: attributeDescriptors)
                {
                	if (firstAttribute)
                		firstAttribute = false;
                	else
                		line.append(",");

                	line.append(feature.getAttribute(attributeDescriptor.getName()));
                }

                if (logger.isLoggable(Level.FINE))
            	    logger.log(Level.FINE, "line = [" + line + "]");

                _dataProvider.produce(line.toString());
            }
        }
        catch (Throwable throwable)
        {
        	logger.log(Level.WARNING, "Problem during conversion of shapefile", throwable);
        }
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
