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

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.impl.InfluxDBResultMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author nghiatc
 * @since Mar 16, 2018
 */
public class StockIFLDAO extends IFLCommon {
    private final Logger logger = LoggerFactory.getLogger(StockIFLDAO.class);
	private static Lock lock = new ReentrantLock();
    private static Map<String, StockIFLDAO> mapInstance = new ConcurrentHashMap<>();
   // private InfluxDBResultMapper resultMapper = new InfluxDBResultMapper(); // thread-safe - can be reused

    private static final String prefix = "uts";
    private String symbol = "";
	private final String TABLE_NAME = "stock_ms";
    
    public StockIFLDAO(String symbol) {
        super(prefix, symbol.toUpperCase());
        this.symbol = symbol.toUpperCase();
        System.out.println("===== Database [" + getDbName() + "] ==> " + "Table[" + TABLE_NAME + "] =====");
    }
    
    public static StockIFLDAO getInstance(String symbol) {
        if (symbol == null || symbol.isEmpty()) {
            return null;
        }
        String key = symbol.toUpperCase();
        StockIFLDAO instance = mapInstance.getOrDefault(key, null);
		if(instance == null) {
			lock.lock();
			try {
				if(instance == null) {
					instance = new StockIFLDAO(symbol);
                    mapInstance.put(key, instance);
				}
			} finally {
				lock.unlock();
			}
		}
		return instance;
	}
    
    public int insert(Stock s) {
		int rs = -1;
		if(client != null) {
			try {
				Point p = toPoint(s);
                client.write(p);
                rs = 0;
			} catch(final Exception e) {
				logger.error("insert: ", e);
			}
		}
		return rs;
	}
    
    public List<Stock> getAll(){
        List<Stock> rs = null;
        if(client != null) {
			try {
				String cmd = "SELECT * FROM " + TABLE_NAME;
                System.out.println("getAll cmd: " + cmd);
                Query query = new Query(cmd, getDbName());
                QueryResult qr = client.query(query);
                rs = parseQueryResult2(qr);
			} catch(final Exception e) {
				logger.error("getAll: ", e);
			}
		}
        return rs;
    }
    
    public List<Stock> getRange(String fromDate, String toDate){
        List<Stock> rs = null;
        if(client != null) {
			try {
				//String cmd = "SELECT * FROM " + TABLE_NAME + " WHERE time >= '" + fromDate + "' and time <= '" + toDate + "';";
                String cmd = "SELECT * FROM " + TABLE_NAME + " WHERE time >= " + fromDate + " and time <= " + toDate + ";";
                //String cmd = "SELECT * FROM " + TABLE_NAME + " WHERE time >= '" + fromDate + "';";
                System.out.println("getRange cmd: " + cmd);
                Query query = new Query(cmd, getDbName());
                QueryResult qr = client.query(query);
                rs = parseQueryResult2(qr);
			} catch(final Exception e) {
				logger.error("getRange: ", e);
			}
		}
        return rs;
    }
    
    public List<Stock> getRangeTimestamp(long fromDate, long toDate){
        List<Stock> rs = null;
        if(client != null) {
			try {
                String cmd = "SELECT * FROM " + TABLE_NAME + " WHERE time >= " + fromDate + "u and time <= " + toDate + "u;";
                System.out.println("getRange cmd: " + cmd);
                Query query = new Query(cmd, getDbName());
                QueryResult qr = client.query(query);
                rs = parseQueryResult2(qr);
			} catch(final Exception e) {
				logger.error("getRangeTimestamp: ", e);
			}
		}
        return rs;
    }
    
    public List<Stock> getRangeGroupBy(String fromDate, String toDate, String typeTime){
        List<Stock> rs = null;
        if(client != null) {
			try {
				String cmd = "SELECT max(price) as max_price, min(price) as min_price, sum(volume) as sum_volume FROM " + TABLE_NAME 
                        + " group by time("+typeTime+")";
                // + " WHERE time >= '" + fromDate + "' and time <= '" + toDate + "' " + "group by time("+typeTime+")";
                System.out.println("getRangeGroupBy cmd: " + cmd);
                Query query = new Query(cmd, getDbName());
                QueryResult qr = client.query(query);
                //rs = parseQueryResult2(qr);
                System.out.println("getRangeGroupBy: " + qr);
			} catch(final Exception e) {
				logger.error("getRangeGroupBy: ", e);
			}
		}
        return rs;
    }
    
    public List<Stock> parseQueryResult2(QueryResult qr){
        List<Stock> rs = new ArrayList<>();
        try {
            if (qr != null) {
                System.out.println("qr: " + qr);
                //System.out.println("JsonQR: " + JsonUtils.Instance.toJson(qr));
                List<QueryResult.Result> lrs =qr.getResults();
                if(lrs != null && !lrs.isEmpty()){
                    for (QueryResult.Result r : lrs) {
                        List<QueryResult.Series> ls = r.getSeries();
                        if(ls != null && !ls.isEmpty()){
                            for (QueryResult.Series s : ls) {
                                List<String> lc = s.getColumns();
                                List<List<Object>> lvl = s.getValues();
                                if (lc != null && !lc.isEmpty() && lvl != null && !lvl.isEmpty()) {
                                    Map<String, Long> mapItem = new HashMap<>();
                                    for (List<Object> lobj : lvl) {
                                        // process 1 row.
                                        if (lobj.size() == lc.size()){
                                            for (int i = 0; i < lc.size(); i++) {
                                                if (i == 0) {
                                                    long time = DTUtil.string2Timestamp(lobj.get(i).toString());
                                                    mapItem.put(lc.get(i), time);
                                                } else{
                                                    long v = (long) Double.parseDouble(lobj.get(i).toString());
                                                    mapItem.put(lc.get(i), v);
                                                }
                                            }
                                            if (mapItem.containsKey("time") && mapItem.containsKey("price")  && mapItem.containsKey("volume")) {
                                                Stock st = new Stock(mapItem.get("time"), mapItem.get("price"), mapItem.get("volume"));
                                                rs.add(st);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rs;
    }
    
    @Measurement(name = "stock_ms")
    public class StockPoint {
        @Column(name = "time")
        public Instant time;
        @Column(name = "price")
        public Long price;
        @Column(name = "volume")
        public Long volume;

        public StockPoint() {
        }

        public StockPoint(Instant timestamp, long price, long volume) {
            this.time = timestamp;
            this.price = price;
            this.volume = volume;
        }

        public Instant getTime() {
            return time;
        }

        public void setTime(Instant time) {
            this.time = time;
        }

        public long getPrice() {
            return price;
        }

        public void setPrice(long price) {
            this.price = price;
        }

        public long getVolume() {
            return volume;
        }

        public void setVolume(long volume) {
            this.volume = volume;
        }

        @Override
        public String toString() {
            return "StockPoint{" + "time=" + time + ", price=" + price + ", volume=" + volume + '}';
        }
    }
    
    public List<Stock> parseQueryResult(QueryResult qr){
        List<Stock> rs = new ArrayList<>();
        if (qr != null) {
            InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();
            System.out.println("qr: " + qr);
            List<StockPoint> listSP = resultMapper.toPOJO(qr, StockPoint.class);
            if (listSP != null && !listSP.isEmpty()) {
                for (StockPoint sp : listSP) {
                    Stock s = new Stock(sp.getTime().toEpochMilli(), (long)sp.getPrice(), (long)sp.getVolume());
                    rs.add(s);
                }
            }
        }
        return rs;
    }
    
    public Point toPoint(Stock s) {
        Point p = Point.measurement(TABLE_NAME)
                    .time(s.getTimestamp(), TimeUnit.MILLISECONDS)
                    //.time(s.getTimestamp(), TimeUnit.NANOSECONDS)
                    .addField("price", s.getPrice())
                    .addField("volume", s.getVolume())
                    .build();
        return p;
    }
}
