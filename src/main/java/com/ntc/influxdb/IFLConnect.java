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

import com.ntc.configer.NConfig;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author nghiatc
 * @since Mar 15, 2018
 */
public class IFLConnect {
    private static final Logger logger = LoggerFactory.getLogger(IFLConnect.class);
	private static final ConcurrentHashMap<String, IFLConnect> instanceMap = new ConcurrentHashMap<>(16, 0.9f, 16);
	private static Lock lock = new ReentrantLock();
    
    private InfluxDB client;
    private String host;
    private String dbName;

    public InfluxDB getClient() {
        return client;
    }

    public String getHost() {
        return host;
    }

    public String getDbName() {
        return dbName;
    }

    public IFLConnect(String prefix) {
        host = NConfig.getConfig().getString(prefix + ".influxdb.host", "http://172.17.0.1:8086");
        dbName = NConfig.getConfig().getString(prefix + ".influxdb.db", "stockpm");
        String rpName = "defaultPolicy";
        client = InfluxDBFactory.connect(host);
        client.createDatabase(dbName);
        client.setDatabase(dbName);
        client.createRetentionPolicy(rpName, dbName, "30d", "30m", 2, true);
        client.setRetentionPolicy(rpName);
        // Flush every 2000 Points, at least every 100ms
        client.enableBatch(BatchOptions.DEFAULTS
                                        //.actions(1)
                                        //.flushDuration(100)
                                        .consistency(InfluxDB.ConsistencyLevel.ALL)
        );
        
        if(client == null) {
			logger.error("Don't found influxDB");
		}
    }
    
    public IFLConnect(String prefix, String symbol) {
        host = NConfig.getConfig().getString(prefix + ".influxdb.host", "http://172.17.0.1:8086");
        dbName = NConfig.getConfig().getString(prefix + ".influxdb.db", "stockpm") + "_" + symbol.toUpperCase();
        String rpName = "defaultPolicy";
        client = InfluxDBFactory.connect(host);
        client.createDatabase(dbName);
        client.setDatabase(dbName);
        client.createRetentionPolicy(rpName, dbName, "30d", "30m", 2, true);
        client.setRetentionPolicy(rpName);
        // Flush every 2000 Points, at least every 100ms
        client.enableBatch(BatchOptions.DEFAULTS
                                        //.actions(1)
                                        //.flushDuration(100)
                                        .consistency(InfluxDB.ConsistencyLevel.ALL)
        );
        
        if(client == null) {
			logger.error("Don't found influxDB");
		}
    }
    
    public static IFLConnect getInstance(String prefix){
        IFLConnect instance = instanceMap.get(prefix);
		if(instance == null) {
			lock.lock();
			try {
				instance = instanceMap.get(prefix);
				if(instance == null) {
					try {
						instance = new IFLConnect(prefix);
						instanceMap.put(prefix, instance);
					} catch(Exception e) {
						logger.error("error ", e);
					}
				}
			} finally {
				lock.unlock();
			}
		}
		return instance;
    }
    
    public static IFLConnect getInstance(String prefix, String symbol){
        IFLConnect instance = instanceMap.get(prefix);
		if(instance == null) {
			lock.lock();
			try {
				instance = instanceMap.get(prefix);
				if(instance == null) {
					try {
						instance = new IFLConnect(prefix, symbol);
						instanceMap.put(prefix, instance);
					} catch(Exception e) {
						logger.error("error ", e);
					}
				}
			} finally {
				lock.unlock();
			}
		}
		return instance;
    }
}
