/*
 * Copyright (c) 2014-2016, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.geotools;

import java.util.Collections;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import com.arjuna.databroker.data.DataFlowNodeFactory;
import com.arjuna.databroker.data.DataFlowNodeFactoryInventory;
import com.arjuna.dbplugins.geotools.csv2shp.CSV2ShapeFileConverterDataProcessorFactory;
import com.arjuna.dbplugins.geotools.shp2csv.ShapeFile2CSVConverterDataProcessorFactory;

@Startup
@Singleton
public class GeoToolsDataFlowNodeFactoriesSetup
{
    @PostConstruct
    public void setup()
    {
        DataFlowNodeFactory csv2ShapeFileConverterDataProcessorFactory = new CSV2ShapeFileConverterDataProcessorFactory("CSV 2 ShapeFile Converter Data Processor Factory", Collections.<String, String>emptyMap());
        DataFlowNodeFactory shapeFile2CSVConverterDataProcessorFactory = new ShapeFile2CSVConverterDataProcessorFactory("ShapeFile 2 CSV Converter Data Processor Factory", Collections.<String, String>emptyMap());

        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(csv2ShapeFileConverterDataProcessorFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(shapeFile2CSVConverterDataProcessorFactory);
    }

    @PreDestroy
    public void cleanup()
    {
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("CSV 2 ShapeFile Converter Data Processor Factory");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("ShapeFile 2 CSV Converter Data Processor Factory");
    }

    @EJB(lookup="java:global/databroker/control-core/DataFlowNodeFactoryInventory")
    private DataFlowNodeFactoryInventory _dataFlowNodeFactoryInventory;
}
