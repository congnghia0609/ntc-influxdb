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

import org.influxdb.InfluxDB;

/**
 *
 * @author nghiatc
 * @since Mar 16, 2018
 */
public class IFLCommon {
    private final IFLConnect connect;
    protected InfluxDB client;

    public IFLCommon() {
        String prefix = "uts";
        connect = IFLConnect.getInstance(prefix);
        if (connect != null) {
            client = connect.getClient();
        }
    }
    
    public IFLCommon(String prefix) {
        connect = IFLConnect.getInstance(prefix);
        if (connect != null) {
            client = connect.getClient();
        }
    }
    
    public IFLCommon(String prefix, String symbol) {
        connect = IFLConnect.getInstance(prefix, symbol);
        if (connect != null) {
            client = connect.getClient();
        }
    }

    public IFLConnect getConnect() {
        return connect;
    }

    public InfluxDB getClient() {
        return client;
    }
    
    public String getDbName() {
        return connect.getDbName();
    }
    
}
