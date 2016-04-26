/*
 * Copyright (c) 2014-2016, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.geotools.csv2shp;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringBufferInputStream;
import java.io.StringReader;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
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
import com.arjuna.databroker.data.jee.annotation.PostActivated;
import com.arjuna.databroker.data.jee.annotation.PostConfig;
import com.arjuna.databroker.data.jee.annotation.PostCreated;
import com.arjuna.databroker.data.jee.annotation.PostRecovery;

public class CSV2ShapeFileConverterDataProcessor implements DataProcessor
{
    private static final Logger logger = Logger.getLogger(CSV2ShapeFileConverterDataProcessor.class.getName());

    public static final String LONGITUDECOLUMN_PROPERTYNAME_PREFIX = "Longitude Column";
    public static final String LATITUDECOLUMN_PROPERTYNAME_PREFIX  = "Latitude Column";

    public CSV2ShapeFileConverterDataProcessor()
    {
        logger.log(Level.INFO, "CSV2ShapeFileConverterDataProcessor");

        _longitudeColumn = -1;
        _latitudeColumn  = -1;
    }

    public CSV2ShapeFileConverterDataProcessor(String name, Map<String, String> properties)
    {
        logger.log(Level.INFO, "CSV2ShapeFileConverterDataProcessor: " + name + ", " + properties);

        _name       = name;
        _properties = properties;

        _longitudeColumn = -1;
        _latitudeColumn  = -1;
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

    @PostCreated
    @PostRecovery
    @PostConfig
    public void setup()
    {
        try
        {
            _longitudeColumn = Integer.parseInt(LONGITUDECOLUMN_PROPERTYNAME_PREFIX);
            _latitudeColumn  = Integer.parseInt(LATITUDECOLUMN_PROPERTYNAME_PREFIX);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem during parsing of properties", throwable);

            _longitudeColumn = -1;
            _latitudeColumn  = -1;
        }
    }

    public void convert(String data)
    {
        logger.log(Level.INFO, "CSV2ShapeFileConverterDataProcessor.convert");

        try
        {
            Reader    csvReader = new StringReader(data);
            CSVParser csvParser = new CSVParser(csvReader, CSVFormat.EXCEL);

            StringBuffer shapefile = new StringBuffer();

            for (CSVRecord record : csvParser)
            {

            }

            _dataProvider.produce(shapefile.toString());
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

    private int _longitudeColumn;
    private int _latitudeColumn;

    private DataFlow             _dataFlow;
    private String               _name;
    private Map<String, String>  _properties;
    @DataConsumerInjection(methodName="consume")
    private DataConsumer<File>   _dataConsumer;
    @DataProviderInjection
    private DataProvider<String> _dataProvider;
}
