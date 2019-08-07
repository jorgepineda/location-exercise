package com.qriously.location;

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MultithreadedCountyResolver implements CountyResolver {

    private static final String TEMP_DIR = "java.io.tmpdir";
    private static final String[] SHAPE_FILE_EXTENSIONS = new String[]{ ".dbf", ".prj", ".shp", ".shx" };
    private final static String USA_COUNTIES = "usa_counties";

    private String shapeFilePath;
    private int numberThreads = 100;
    private int batchSize = 1000;

    public MultithreadedCountyResolver() {
        // Default constructor to use the default  multi-threading parameters
    }

    public MultithreadedCountyResolver(int numberOfThreads, int pointsBatchSize) {
        // Constructor to allow the manipulation of multi-threading parameters when invoking
        this.numberThreads = numberOfThreads;
        this.batchSize = pointsBatchSize;
    }

    @Override
    public Map<String, Integer> resolve(CoordinateSupplier coordinateSupplier) throws ResolverException {
        try {
            // Boilerplate code to read the data
            shapeFilePath = extractShapeFiles(USA_COUNTIES);
            FileDataStore store = FileDataStoreFinder.getDataStore(new File(shapeFilePath + ".shp"));
            SimpleFeatureSource featureSource = store.getFeatureSource();
            String geometryPropertyName = featureSource.getSchema().getGeometryDescriptor().getLocalName();
            GeometryFactory gf=new GeometryFactory();

            Map<String, AtomicInteger> result = new HashMap<>();

            // Buffers the county polygons in a map
            Map<String, MultiPolygon> featuresMap = new HashMap<>();
            SimpleFeatureCollection countyFeatures = featureSource.getFeatures(Query.ALL);
            SimpleFeatureIterator countyFeatureIterator = countyFeatures.features();
            while (countyFeatureIterator.hasNext()) {
                SimpleFeature countyFeature = countyFeatureIterator.next();
                String countyCode = (String) countyFeature.getAttribute("LVL_2_ID");
                MultiPolygon countyPolygon = (MultiPolygon)countyFeature.getAttribute(geometryPropertyName);
                featuresMap.put(countyCode, countyPolygon);
            }
            countyFeatureIterator.close();


            ThreadPoolExecutor executor =
                    (ThreadPoolExecutor) Executors.newFixedThreadPool(numberThreads);

            // Process the points in batches. Each batch running in a separate thread
            Coordinate coordinate = null;
            List<Point> pointList =  new ArrayList<>();
            int numPoints = 0;
            while ((coordinate = coordinateSupplier.get()) != null) {
                Point point = gf.createPoint(new com.vividsolutions.jts.geom.Coordinate(coordinate.longitude, coordinate.latitude));
                pointList.add(point);
                if (++numPoints % batchSize == 0) {
                    List<Point> batchPointList = pointList;
                    executor.submit(() -> {
                        processBatch(featuresMap, batchPointList, result);
                    });
                    pointList =  new ArrayList<>();
                }
            }
            if (numPoints % batchSize != 0) {
                List<Point> executorPointList = pointList;
                executor.submit(() -> {
                    processBatch(featuresMap, executorPointList, result);
                });
            }

            // Waits for the executor to finish smoothly
            executor.shutdown();
            try {
                executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
            }

            return result.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));

        } catch (IOException e) {
            throw new ResolverException(e);
        }
    }

    private void processBatch(Map<String, MultiPolygon> featuresMap, List<Point> batchPointList, Map<String, AtomicInteger> result) {
        for (String countyCode:featuresMap.keySet()) {
            for (Point p:batchPointList) {
                if (featuresMap.get(countyCode).contains(p)) {
                    synchronized (result) {
                        result.computeIfAbsent(countyCode, key -> new AtomicInteger(0)).incrementAndGet();
                    }
                }
            }
        }
    }

    /**
     * Extract shapefiles from bundled resources to a temporary location
     *
     * Returns the filesystem path of shapefile without file-extension
     */
    private String extractShapeFiles(String shapeFileWithoutExtension) throws IOException {
        String shapeFilePathRoot = Paths.get(
                System.getProperty(TEMP_DIR),
                shapeFileWithoutExtension + "-" + System.currentTimeMillis()).toString();

        for (String extension : SHAPE_FILE_EXTENSIONS) {
            File file = new File(shapeFilePathRoot + extension);
            byte[] buffer = new byte[1024];
            try (InputStream in = getClass().getResourceAsStream("/" + shapeFileWithoutExtension + extension);
                OutputStream out = new FileOutputStream(file)) {
                int read;
                while ((read = in.read(buffer)) > 0) {
                    out.write(buffer, 0, read);
                }
            }
        }

        return shapeFilePathRoot;
    }


    @Override
    public void close() {
        for (String extension : SHAPE_FILE_EXTENSIONS) {
            File file = new File(shapeFilePath + extension);
            if (file.exists()) {
                file.delete();
            }
        }
    }
}

