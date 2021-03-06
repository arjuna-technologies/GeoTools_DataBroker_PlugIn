/*
 * Copyright (c) 2014-2016, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.geotools.shp2csv;

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
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataFlow;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataProcessor;
import com.arjuna.databroker.data.jee.annotation.DataConsumerInjection;
import com.arjuna.databroker.data.jee.annotation.DataProviderInjection;

public class ShapeFile2CSVConverterDataProcessor implements DataProcessor
{
    private static final Logger logger = Logger.getLogger(ShapeFile2CSVConverterDataProcessor.class.getName());

    public ShapeFile2CSVConverterDataProcessor()
    {
        logger.log(Level.INFO, "ShapeFile2CSVConverterDataProcessor");

        _name       = null;
        _properties = null;
    }

    public ShapeFile2CSVConverterDataProcessor(String name, Map<String, String> properties)
    {
        logger.log(Level.INFO, "ShapeFile2CSVConverterDataProcessor: " + name + ", " + properties);

        _name       = name;
        _properties = properties;
    }

    @Override
    public DataFlow getDataFlow()
    {
        return _dataFlow;
    }

    @Override
    public void setDataFlow(DataFlow dataFlow)
    {
        _dataFlow = dataFlow;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public void setName(String name)
    {
        _name = name;
    }

    @Override
    public Map<String, String> getProperties()
    {
        return Collections.unmodifiableMap(_properties);
    }

    @Override
    public void setProperties(Map<String, String> properties)
    {
        _properties = properties;
    }

    public void convert(File shapefileFile)
    {
        logger.log(Level.INFO, "ShapeFile2CSVConverterDataProcessor.convert: " + shapefileFile.getName());

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

        dataConsumerDataClasses.add(String.class);

        return dataConsumerDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataConsumer<T> getDataConsumer(Class<T> dataClass)
    {
        if (dataClass == String.class)
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

    private DataFlow             _dataFlow;
    private String               _name;
    private Map<String, String>  _properties;
    @DataConsumerInjection(methodName="consume")
    private DataConsumer<String> _dataConsumer;
    @DataProviderInjection
    private DataProvider<String> _dataProvider;
}
