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

import java.util.Calendar;
import java.util.List;
import java.util.Random;

/**
 *
 * @author nghiatc
 * @since Mar 19, 2018
 */
public class TestStock {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            String symbol = "BTC_ETH";
            // Insert Data.
//            int n = 10;
//            for (int i = 0; i < n; i++) {
//                long price = randomRange(1, 100);
//                long volume = randomRange(1, 10);
//                Stock s = new Stock(System.currentTimeMillis(), price, volume);
//                int err = StockDAO.getInstance(symbol).insert(s);
//                System.out.println("err: " + err + " || " + s.toString());
//                Thread.sleep(10);
//            }
//            Thread.sleep(2000);
            
            // Get Data.
            List<Stock> allStock = StockIFLDAO.getInstance(symbol).getAll();
            System.out.println("allStock: " + allStock);
            
            if (allStock != null && !allStock.isEmpty()){
                for (Stock s : allStock) {
                    System.out.println("s: " + s.toStringNice());
                }
            }
            
            //Get Range
            Calendar fcal = Calendar.getInstance();
            //fcal.set(Calendar.DAY_OF_YEAR, fcal.get(Calendar.DAY_OF_YEAR) - 1);
            //fcal.set(Calendar.HOUR_OF_DAY, fcal.get(Calendar.HOUR_OF_DAY) - 4);
            //fcal.set(Calendar.HOUR_OF_DAY, fcal.get(Calendar.HOUR_OF_DAY) - 1);
            fcal.set(Calendar.MINUTE, fcal.get(Calendar.MINUTE) - 20);
            Calendar tcal = Calendar.getInstance();
            //String fromDate = DTUtil.formatInputIFL(fcal);
            //String toDate = DTUtil.formatInputIFL(tcal);
            //String fromDate = fcal.getTimeInMillis() + "u";
            //String toDate = tcal.getTimeInMillis() + "u";
            long fromDate = fcal.getTimeInMillis();
            long toDate = tcal.getTimeInMillis();
            //List<Stock> rangeStock = StockDAO.getInstance(symbol).getRange(fromDate, toDate);
            List<Stock> rangeStock = StockIFLDAO.getInstance(symbol).getRangeTimestamp(fromDate, toDate);
            System.out.println("rangeStock: " + rangeStock);
            
            // getRangeGroupBy
//            String typeTime = "1m";
//            List<Stock> rangeGroupStock = StockDAO.getInstance(symbol).getRangeGroupBy(fromDate, toDate, typeTime);
//            System.out.println("rangeGroupStock: " + rangeGroupStock);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static Random rd = new Random();
    public static int randomRange(int min, int max){
        return rd.nextInt((max - min) + 1) + min;
    }

}
