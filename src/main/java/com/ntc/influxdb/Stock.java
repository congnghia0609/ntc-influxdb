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

import java.text.ParseException;

/**
 *
 * @author nghiatc
 * @since Mar 16, 2018
 */
public class Stock {
    private long timestamp;
    private long price;
    private long volume;

    public Stock(long timestamp, long price, long volume) {
        this.timestamp = timestamp;
        this.price = price;
        this.volume = volume;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
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
        return "Stock{" + "timestamp=" + timestamp + ", price=" + price + ", volume=" + volume + '}';
    }
    
    public String toStringNice() throws ParseException {
        return "Stock{" + "timestamp=" + DTUtil.timestamp2StringIFL(timestamp) + ", price=" + price + ", volume=" + volume + '}';
    }
}
