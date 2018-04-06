/*
 * Copyright 2018 nghiatc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ntc.influxdb;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;

/**
 *
 * @author nghiatc
 * @since Mar 15, 2018
 */
public class TestInfluxDB {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            InfluxDB influxDB = InfluxDBFactory.connect("http://172.17.0.1:8086");
            String dbName = "aTimeSeries";
            influxDB.createDatabase(dbName);
            influxDB.setDatabase(dbName);
            String rpName = "aRetentionPolicy";
            influxDB.createRetentionPolicy(rpName, dbName, "30d", "30m", 2, true);
            influxDB.setRetentionPolicy(rpName);

            // Flush every 2000 Points, at least every 100ms
            //influxDB.enableBatch(BatchOptions.DEFAULTS.actions(1).flushDuration(100));

            BatchPoints batchPoints = BatchPoints
                                                .database(dbName)
                                                //.tag("async", "true")
                                                .retentionPolicy(rpName)
                                                .consistency(ConsistencyLevel.ALL)
                                                .build();
            Point point1 = Point.measurement("cpu")
                                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                                .addField("idle", 90L)
                                .addField("user", 9L)
                                .addField("system", 1L)
                                .build();
            Point point2 = Point.measurement("cpu")
                                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                                .addField("idle", 90L)
                                .addField("user", 9L)
                                .addField("system", 1L)
                                .build();
            
            Point point3 = Point.measurement("disk")
                                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                                .addField("used", 80L)
                                .addField("free", 1L)
                                .build();
            batchPoints.point(point1);
            batchPoints.point(point2);
            influxDB.write(batchPoints);
            Thread.sleep(2000);

            Query query = new Query("SELECT * FROM cpu", dbName);
            QueryResult qr = influxDB.query(query);
            Thread.sleep(2000);
            System.out.println("qr: " + qr);
            List<Result> lrs =qr.getResults();
            if(lrs != null && !lrs.isEmpty()){
                for (Result r : lrs) {
                    List<Series> ls = r.getSeries();
                    if(ls != null && !ls.isEmpty()){
                        for (Series s : ls) {
                            List<String> lc = s.getColumns();
                            System.out.println("Name:" + s.getName() + " - Value: " + s.getValues());
                        }
                    }
                }
            }
            
            influxDB.query(query, 20, queryResult -> System.out.println(queryResult));
            Thread.sleep(2000);

            influxDB.dropRetentionPolicy(rpName, dbName);
            influxDB.deleteDatabase(dbName);
            influxDB.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
