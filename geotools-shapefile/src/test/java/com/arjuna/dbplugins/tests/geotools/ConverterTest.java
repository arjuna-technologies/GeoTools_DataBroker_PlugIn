/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.tests.geotools;

import java.util.Collections;
import org.junit.Test;
import static org.junit.Assert.*;
import com.arjuna.dbplugins.geotools.dataflownodes.ShapeFileConverterDataProcessor;

public class ConverterTest
{
    @Test
    public void simplestConversion()
    {
        ShapeFileConverterDataProcessor shapeFileDataProcessor = new ShapeFileConverterDataProcessor("ShapeFile Data Processor", Collections.<String, String>emptyMap());
    }
}
