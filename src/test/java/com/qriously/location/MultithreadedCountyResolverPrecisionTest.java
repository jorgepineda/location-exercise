package com.qriously.location;

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.GeometryDescriptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.*;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;

@RunWith(PowerMockRunner.class)
@PrepareForTest({MultithreadedCountyResolver.class,
        FileDataStoreFinder.class})
public class MultithreadedCountyResolverPrecisionTest {

    /*
    As a precision test, the following scenario was prepared:

    Two separated squares labeled "CountyCode1" and "CountyCode2" and 31 points in the diagonal as follows

                        |---------*
                        |        *|
                        |       * |
                        |      *  |
                        |     *   |
                        |    *    |
                        |   *     |
                        |  *      |
                        | *       |
                        |*        |
                        *---------|
                       *
                      *
                     *
                    *
                   *
                  *
                 *
                *
               *
    |---------*
    |        *|
    |       * |
    |      *  |
    |     *   |
    |    *    |
    |   *     |
    |  *      |
    | *       |
    |*        |
    *---------|

   it is expected that "CountyCode1" contains 9 points and "CountyCode2" 9 points (Points in the border are not considered contained),
   4 points are in the border and 9 are external to both polygons.
*/
    @Test
    public void testPrecisionForImplementedCountyResolver() throws Exception {
        // Given
        MultithreadedCountyResolver underTestSpy = spy(new MultithreadedCountyResolver());
        PowerMockito.doReturn("filePath").when(underTestSpy, "extractShapeFiles", anyString());

        PowerMockito.mockStatic(FileDataStoreFinder.class);
        FileDataStore store = mock(FileDataStore.class);
        given(FileDataStoreFinder.getDataStore(any(File.class))).willReturn(store);
        SimpleFeatureSource featureSource = mock(SimpleFeatureSource.class);
        given(store.getFeatureSource()).willReturn(featureSource);
        SimpleFeatureType schema = mock(SimpleFeatureType.class);
        given(featureSource.getSchema()).willReturn(schema);
        GeometryDescriptor geometryDescriptor = mock(GeometryDescriptor.class);
        given(schema.getGeometryDescriptor()).willReturn(geometryDescriptor);
        given(geometryDescriptor.getLocalName()).willReturn("geographic_fetaure_name");

        SimpleFeatureCollection countyFeatures = mock(SimpleFeatureCollection.class);
        given(featureSource.getFeatures(Query.ALL)).willReturn(countyFeatures);
        SimpleFeatureIterator countyFeatureIterator = mock(SimpleFeatureIterator.class);
        given(countyFeatures.features()).willReturn(countyFeatureIterator);

        given(countyFeatureIterator.hasNext()).willReturn(true, true, false);
        SimpleFeature countyFeature1 = mock(SimpleFeature.class);
        SimpleFeature countyFeature2 = mock(SimpleFeature.class);
        given(countyFeatureIterator.next()).willReturn(countyFeature1, countyFeature2);

        given(countyFeature1.getAttribute("LVL_2_ID")).willReturn("CountyCode1");
        given(countyFeature2.getAttribute("LVL_2_ID")).willReturn("CountyCode2");

        Polygon poligon1 = toPolygon(1000.0, 1000.0, 1000.0, 1000.0);
        MultiPolygon MultiPoligon1 = new MultiPolygon(new Polygon [] {poligon1}, new GeometryFactory());
        Polygon poligon2 = toPolygon(3000.0, 3000.0, 1000.0, 1000.0);
        MultiPolygon MultiPoligon2 = new MultiPolygon(new Polygon [] {poligon2}, new GeometryFactory());

        given(countyFeature1.getAttribute("geographic_fetaure_name")).willReturn(MultiPoligon1);
        given(countyFeature2.getAttribute("geographic_fetaure_name")).willReturn(MultiPoligon2);

        willDoNothing().given(countyFeatureIterator).close();

        CoordinateSupplier coordinateSupplier = mock(CoordinateSupplier.class);
        given(coordinateSupplier.get()).willReturn(
                new Coordinate(1000.0, 1000.0),
                new Coordinate(1100.0, 1100.0),
                new Coordinate(1200.0, 1200.0),
                new Coordinate(1300.0, 1300.0),
                new Coordinate(1400.0, 1400.0),
                new Coordinate(1500.0, 1500.0),
                new Coordinate(1600.0, 1600.0),
                new Coordinate(1700.0, 1700.0),
                new Coordinate(1800.0, 1800.0),
                new Coordinate(1900.0, 1900.0),
                new Coordinate(2000.0, 2000.0),
                new Coordinate(2100.0, 2100.0),
                new Coordinate(2200.0, 2200.0),
                new Coordinate(2300.0, 2300.0),
                new Coordinate(2400.0, 2400.0),
                new Coordinate(2500.0, 2500.0),
                new Coordinate(2600.0, 2600.0),
                new Coordinate(2700.0, 2700.0),
                new Coordinate(2800.0, 2800.0),
                new Coordinate(2900.0, 2900.0),
                new Coordinate(3000.0, 3000.0),
                new Coordinate(3100.0, 3100.0),
                new Coordinate(3200.0, 3200.0),
                new Coordinate(3300.0, 3300.0),
                new Coordinate(3400.0, 3400.0),
                new Coordinate(3500.0, 3500.0),
                new Coordinate(3600.0, 3600.0),
                new Coordinate(3700.0, 3700.0),
                new Coordinate(3800.0, 3800.0),
                new Coordinate(3900.0, 3900.0),
                new Coordinate(4000.0, 4000.0),
                null);

        // When
        Map<String, Integer> result = underTestSpy.resolve(coordinateSupplier);

        // Then
        assertNotNull(result);
        assertEquals(result.size(), 2);
        assertNotNull(result.get("CountyCode1"));
        assertEquals(Optional.ofNullable(result.get("CountyCode1")), Optional.of(9));
        assertNotNull(result.get("CountyCode2"));
        assertEquals(Optional.ofNullable(result.get("CountyCode1")), Optional.of(9));
    }

    // Code copied from http://www.javased.com/index.php?api=com.vividsolutions.jts.geom.Polygon
    private Polygon toPolygon(double x,double y,double w,double h){
        GeometryFactory factory = new GeometryFactory();
        com.vividsolutions.jts.geom.Coordinate[] coordinates=new com.vividsolutions.jts.geom.Coordinate[5];
        coordinates[0]=new com.vividsolutions.jts.geom.Coordinate(x,y);
        coordinates[1]=new com.vividsolutions.jts.geom.Coordinate(x + w,y);
        coordinates[2]=new com.vividsolutions.jts.geom.Coordinate(x + w,y + h);
        coordinates[3]=new com.vividsolutions.jts.geom.Coordinate(x,y + h);
        coordinates[4]=new com.vividsolutions.jts.geom.Coordinate(x,y);
        LinearRing lr=factory.createLinearRing(coordinates);
        Polygon polygon=factory.createPolygon(lr,new LinearRing[]{});
        return polygon;
    }

}