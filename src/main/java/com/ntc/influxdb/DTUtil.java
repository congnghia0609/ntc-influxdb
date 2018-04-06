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

import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;

/**
 *
 * @author nghiatc
 * @since Mar 19, 2018
 */
public class DTUtil {
    public static SimpleDateFormat sdfx = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    public static SimpleDateFormat sdfz = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    public static ISO8601DateFormat df = new ISO8601DateFormat();
    public static SimpleDateFormat sdfIFL = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    
    public static long string2Timestamp(String stime) throws ParseException{
        Date d = Date.from(Instant.parse(stime));
        return d.getTime();
    }
    
    public static String timestamp2String(long ltime) throws ParseException{
        Date d = Date.from(Instant.ofEpochMilli(ltime));
        return sdfx.format(d);
    }
    
    public static String timestamp2StringIFL(long ltime) throws ParseException{
        Date d = Date.from(Instant.ofEpochMilli(ltime));
        return sdfIFL.format(d);
    }
    
    public static String formatInputIFL(Calendar cal) throws ParseException{
        Date d = cal.getTime();
        return sdfIFL.format(d);
    }
    
    public static long string2TimestampJson(String stime) throws ParseException{
        Date d = df.parse(stime);
        return d.getTime();
    }
    
    public static enum TYPE_TIME {
        ONE_MINUTE(0),         // 1 phút.
        FIVE_MINUTE(1),        // 5 phút.
        FIFTEEN_MINUTE(2),     // 15 phút.
        THIRTY_MINUTE(3),      // 30 phút.
        ONE_HOUR(4),           // 1 giờ.
        TWO_HOUR(5),           // 2 giờ.
        FOUR_HOUR(6),          // 4 giờ.
        SIX_HOUR(7),           // 6 giờ.
        TWELVE_HOUR(8),        // 12 giờ.
        ONE_DAY(9),            // 1 ngày.
        ONE_WEEK(10);          // 1 tuần.
        
        private final int value;

        private TYPE_TIME(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
        
        public String getName(){
            return findByValue(value).name();
        }
        
        public String getOtherName(){
            return defineName(value);
        }
        
        public static boolean isExistOtherName(String name){
            for (TYPE_TIME tt : TYPE_TIME.values()) {
                if (tt.getOtherName().equals(name)) {
                    return true;
                }
            }
            return false;
        }
        
        public static String defineName(int value) {
            switch (value) {
                case 0:
                    return "1m";
                case 1:
                    return "5m";
                case 2:
                    return "15m";
                case 3:
                    return "30m";
                case 4:
                    return "1h";
                case 5:
                    return "2h";
                case 6:
                    return "4h";
                case 7:
                    return "6h";
                case 8:
                    return "12h";
                case 9:
                    return "1d";
                case 10:
                    return "1w";
                default:
                    return null;
            }
        }
        
        public static TYPE_TIME findByValue(int value) {
            switch (value) {
                case 0:
                    return ONE_MINUTE;
                case 1:
                    return FIVE_MINUTE;
                case 2:
                    return FIFTEEN_MINUTE;
                case 3:
                    return THIRTY_MINUTE;
                case 4:
                    return ONE_HOUR;
                case 5:
                    return TWO_HOUR;
                case 6:
                    return FOUR_HOUR;
                case 7:
                    return SIX_HOUR;
                case 8:
                    return TWELVE_HOUR;
                case 9:
                    return ONE_DAY;
                case 10:
                    return ONE_WEEK;
                default:
                    return null;
            }
        }
    }
    
    public static long createKeyTimestamp(Calendar c, TYPE_TIME type){
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(c.getTimeInMillis());
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        switch(type) {
            case ONE_MINUTE: { // 1 phút.
                return cal.getTimeInMillis();
            }
            case FIVE_MINUTE: { // 5 phút.
                int count = cal.get(Calendar.MINUTE) / 5;
                cal.set(Calendar.MINUTE, count * 5);
                return cal.getTimeInMillis();
            }
            case FIFTEEN_MINUTE: { // 15 phút
                int count = cal.get(Calendar.MINUTE) / 15;
                cal.set(Calendar.MINUTE, count * 15);
                return cal.getTimeInMillis();
            }
            case THIRTY_MINUTE: { // 30 phút
                int count = cal.get(Calendar.MINUTE) / 30;
                cal.set(Calendar.MINUTE, count * 30);
                return cal.getTimeInMillis();
            }
            case ONE_HOUR: { // 1 giờ
                cal.set(Calendar.MINUTE, 0);
                return cal.getTimeInMillis();
            }
            case TWO_HOUR: { // 2 giờ
                int count = cal.get(Calendar.HOUR_OF_DAY) / 2;
                cal.set(Calendar.HOUR_OF_DAY, count * 2);
                cal.set(Calendar.MINUTE, 0);
                return cal.getTimeInMillis();
            }
            case FOUR_HOUR: { // 4 giờ
                int count = cal.get(Calendar.HOUR_OF_DAY) / 4;
                cal.set(Calendar.HOUR_OF_DAY, count * 4);
                cal.set(Calendar.MINUTE, 0);
                return cal.getTimeInMillis();
            }
            case SIX_HOUR: { // 6 giờ
                int count = cal.get(Calendar.HOUR_OF_DAY) / 6;
                cal.set(Calendar.HOUR_OF_DAY, count * 6);
                cal.set(Calendar.MINUTE, 0);
                return cal.getTimeInMillis();
            }
            case TWELVE_HOUR: { // 12 giờ
                int count = cal.get(Calendar.HOUR_OF_DAY) / 12;
                cal.set(Calendar.HOUR_OF_DAY, count * 12);
                cal.set(Calendar.MINUTE, 0);
                return cal.getTimeInMillis();
            }
            case ONE_DAY: { // 1 ngày
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.HOUR_OF_DAY, 0);
                return cal.getTimeInMillis();
            }
            case ONE_WEEK: { // 1 tuần
                cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.HOUR_OF_DAY, 0);
                return cal.getTimeInMillis();
            }
            default: {
                return cal.getTimeInMillis();
            }
        }
    }
    
    public static long createKeyTimestamp(long timestamp, TYPE_TIME type){
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timestamp);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        switch(type) {
            case ONE_MINUTE: { // 1 phút.
                return cal.getTimeInMillis();
            }
            case FIVE_MINUTE: { // 5 phút.
                int count = cal.get(Calendar.MINUTE) / 5;
                cal.set(Calendar.MINUTE, count * 5);
                return cal.getTimeInMillis();
            }
            case FIFTEEN_MINUTE: { // 15 phút
                int count = cal.get(Calendar.MINUTE) / 15;
                cal.set(Calendar.MINUTE, count * 15);
                return cal.getTimeInMillis();
            }
            case THIRTY_MINUTE: { // 30 phút
                int count = cal.get(Calendar.MINUTE) / 30;
                cal.set(Calendar.MINUTE, count * 30);
                return cal.getTimeInMillis();
            }
            case ONE_HOUR: { // 1 giờ
                cal.set(Calendar.MINUTE, 0);
                return cal.getTimeInMillis();
            }
            case TWO_HOUR: { // 2 giờ
                int count = cal.get(Calendar.HOUR_OF_DAY) / 2;
                cal.set(Calendar.HOUR_OF_DAY, count * 2);
                cal.set(Calendar.MINUTE, 0);
                return cal.getTimeInMillis();
            }
            case FOUR_HOUR: { // 4 giờ
                int count = cal.get(Calendar.HOUR_OF_DAY) / 4;
                cal.set(Calendar.HOUR_OF_DAY, count * 4);
                cal.set(Calendar.MINUTE, 0);
                return cal.getTimeInMillis();
            }
            case SIX_HOUR: { // 6 giờ
                int count = cal.get(Calendar.HOUR_OF_DAY) / 6;
                cal.set(Calendar.HOUR_OF_DAY, count * 6);
                cal.set(Calendar.MINUTE, 0);
                return cal.getTimeInMillis();
            }
            case TWELVE_HOUR: { // 12 giờ
                int count = cal.get(Calendar.HOUR_OF_DAY) / 12;
                cal.set(Calendar.HOUR_OF_DAY, count * 12);
                cal.set(Calendar.MINUTE, 0);
                return cal.getTimeInMillis();
            }
            case ONE_DAY: { // 1 ngày
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.HOUR_OF_DAY, 0);
                return cal.getTimeInMillis();
            }
            case ONE_WEEK: { // 1 tuần
                cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.HOUR_OF_DAY, 0);
                return cal.getTimeInMillis();
            }
            default: {
                return cal.getTimeInMillis();
            }
        }
    }
    
    public static long createKeyTimestampCurrent(TYPE_TIME type){
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        switch(type) {
            case ONE_MINUTE: { // 1 phút.
                return cal.getTimeInMillis();
            }
            case FIVE_MINUTE: { // 5 phút.
                int count = cal.get(Calendar.MINUTE) / 5;
                cal.set(Calendar.MINUTE, count * 5);
                return cal.getTimeInMillis();
            }
            case FIFTEEN_MINUTE: { // 15 phút
                int count = cal.get(Calendar.MINUTE) / 15;
                cal.set(Calendar.MINUTE, count * 15);
                return cal.getTimeInMillis();
            }
            case THIRTY_MINUTE: { // 30 phút
                int count = cal.get(Calendar.MINUTE) / 30;
                cal.set(Calendar.MINUTE, count * 30);
                return cal.getTimeInMillis();
            }
            case ONE_HOUR: { // 1 giờ
                cal.set(Calendar.MINUTE, 0);
                return cal.getTimeInMillis();
            }
            case TWO_HOUR: { // 2 giờ
                int count = cal.get(Calendar.HOUR_OF_DAY) / 2;
                cal.set(Calendar.HOUR_OF_DAY, count * 2);
                cal.set(Calendar.MINUTE, 0);
                return cal.getTimeInMillis();
            }
            case FOUR_HOUR: { // 4 giờ
                int count = cal.get(Calendar.HOUR_OF_DAY) / 4;
                cal.set(Calendar.HOUR_OF_DAY, count * 4);
                cal.set(Calendar.MINUTE, 0);
                return cal.getTimeInMillis();
            }
            case SIX_HOUR: { // 6 giờ
                int count = cal.get(Calendar.HOUR_OF_DAY) / 6;
                cal.set(Calendar.HOUR_OF_DAY, count * 6);
                cal.set(Calendar.MINUTE, 0);
                return cal.getTimeInMillis();
            }
            case TWELVE_HOUR: { // 12 giờ
                int count = cal.get(Calendar.HOUR_OF_DAY) / 12;
                cal.set(Calendar.HOUR_OF_DAY, count * 12);
                cal.set(Calendar.MINUTE, 0);
                return cal.getTimeInMillis();
            }
            case ONE_DAY: { // 1 ngày
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.HOUR_OF_DAY, 0);
                return cal.getTimeInMillis();
            }
            case ONE_WEEK: { // 1 tuần
                cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.HOUR_OF_DAY, 0);
                return cal.getTimeInMillis();
            }
            default: {
                return cal.getTimeInMillis();
            }
        }
    }
    
    
    
    public static void main(String[] args) {
        try {
            // 1. test_ONE_MINUTE
            //test_ONE_MINUTE();
            
            // 2. test_FIVE_MINUTE
            //test_FIVE_MINUTE();
            
            // 3. test_FIFTEEN_MINUTE
            //test_FIFTEEN_MINUTE();
            
            // 4. test_THIRTY_MINUTE
            //test_THIRTY_MINUTE();
            
            // 5. test_ONE_HOUR
            //test_ONE_HOUR();
            
            // 6. test_TWO_HOUR
            //test_TWO_HOUR();
            
            // 7. test_FOUR_HOUR
            //test_FOUR_HOUR();
            
            // 8. test_SIX_HOUR
            //test_SIX_HOUR();
            
            // 9. test_TWELVE_HOUR
            //test_TWELVE_HOUR();
            
            // 10. test_ONE_DAY
            //test_ONE_DAY();
            
            // 11. test_ONE_WEEK
            //test_ONE_WEEK();
            
            // 12. TYPE_TIME
            //test2();
            
            // 13. test3
            //test3();
            
            // 14. test4
            test4();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void test4(){
        try {
            Calendar cal = Calendar.getInstance();
            for (TYPE_TIME tt : TYPE_TIME.values()) {
                System.out.println(tt.getOtherName() + "\t\t <==> " + createKeyTimestamp(cal, tt));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void test3(){
        try {
            Calendar cal = Calendar.getInstance();
            System.out.println("cal: " + formatInputIFL(cal));
            long tt = cal.getTimeInMillis();
            System.out.println("tt: " + tt);
            long tt_5m = createKeyTimestamp(tt, TYPE_TIME.FIVE_MINUTE);
            System.out.println("tt_5m: " + tt_5m);
            Calendar cal2 = Calendar.getInstance();
            cal2.setTimeInMillis(tt_5m);
            System.out.println("cal2: " + formatInputIFL(cal2));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void test_ONE_WEEK(){
        try {
            for (int i = 0; i < 10; i++) {
                Calendar cal = Calendar.getInstance();
                System.out.println("cal: " + formatInputIFL(cal));
                long one_week = createKeyTimestamp(cal, TYPE_TIME.ONE_WEEK);
                String s = timestamp2StringIFL(one_week);
                System.out.println("one_week: " + one_week + " <==> s: " + s);
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void test_ONE_DAY(){
        try {
            for (int i = 0; i < 10; i++) {
                Calendar cal = Calendar.getInstance();
                System.out.println("cal: " + formatInputIFL(cal));
                long one_day = createKeyTimestamp(cal, TYPE_TIME.ONE_DAY);
                String s = timestamp2StringIFL(one_day);
                System.out.println("one_day: " + one_day + " <==> s: " + s);
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void test_TWELVE_HOUR(){
        try {
            for (int i = 0; i < 10; i++) {
                Calendar cal = Calendar.getInstance();
                System.out.println("cal: " + formatInputIFL(cal));
                long twelve_hour = createKeyTimestamp(cal, TYPE_TIME.TWELVE_HOUR);
                String s = timestamp2StringIFL(twelve_hour);
                System.out.println("twelve_hour: " + twelve_hour + " <==> s: " + s);
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void test_SIX_HOUR(){
        try {
            for (int i = 0; i < 10; i++) {
                Calendar cal = Calendar.getInstance();
                System.out.println("cal: " + formatInputIFL(cal));
                long six_hour = createKeyTimestamp(cal, TYPE_TIME.SIX_HOUR);
                String s = timestamp2StringIFL(six_hour);
                System.out.println("six_hour: " + six_hour + " <==> s: " + s);
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void test_FOUR_HOUR(){
        try {
            for (int i = 0; i < 10; i++) {
                Calendar cal = Calendar.getInstance();
                System.out.println("cal: " + formatInputIFL(cal));
                long four_hour = createKeyTimestamp(cal, TYPE_TIME.FOUR_HOUR);
                String s = timestamp2StringIFL(four_hour);
                System.out.println("four_hour: " + four_hour + " <==> s: " + s);
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void test_TWO_HOUR(){
        try {
            for (int i = 0; i < 10; i++) {
                Calendar cal = Calendar.getInstance();
                System.out.println("cal: " + formatInputIFL(cal));
                long two_hour = createKeyTimestamp(cal, TYPE_TIME.TWO_HOUR);
                String s = timestamp2StringIFL(two_hour);
                System.out.println("two_hour: " + two_hour + " <==> s: " + s);
                Thread.sleep(2000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void test_ONE_HOUR(){
        try {
            for (int i = 0; i < 10; i++) {
                Calendar cal = Calendar.getInstance();
                System.out.println("cal: " + formatInputIFL(cal));
                long one_hour = createKeyTimestamp(cal, TYPE_TIME.ONE_HOUR);
                String s = timestamp2StringIFL(one_hour);
                System.out.println("one_hour: " + one_hour + " <==> s: " + s);
                Thread.sleep(2000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void test_THIRTY_MINUTE(){
        try {
            for (int i = 0; i < 10; i++) {
                Calendar cal = Calendar.getInstance();
                System.out.println("cal: " + formatInputIFL(cal));
                long thirty_minute = createKeyTimestamp(cal, TYPE_TIME.THIRTY_MINUTE);
                String s = timestamp2StringIFL(thirty_minute);
                System.out.println("thirty_minute: " + thirty_minute + " <==> s: " + s);
                Thread.sleep(6000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void test_FIFTEEN_MINUTE(){
        try {
            for (int i = 0; i < 10; i++) {
                Calendar cal = Calendar.getInstance();
                System.out.println("cal: " + formatInputIFL(cal));
                long fifteen_minute = createKeyTimestamp(cal, TYPE_TIME.FIFTEEN_MINUTE);
                String s = timestamp2StringIFL(fifteen_minute);
                System.out.println("fifteen_minute: " + fifteen_minute + " <==> s: " + s);
                Thread.sleep(6000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void test_FIVE_MINUTE(){
        try {
            for (int i = 0; i < 10; i++) {
                Calendar cal = Calendar.getInstance();
                System.out.println("cal: " + formatInputIFL(cal));
                long five_minute = createKeyTimestamp(cal, TYPE_TIME.FIVE_MINUTE);
                String s = timestamp2StringIFL(five_minute);
                System.out.println("five_minute: " + five_minute + " <==> s: " + s);
                Thread.sleep(30000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void test_ONE_MINUTE(){
        try {
            for (int i = 0; i < 10; i++) {
                Calendar cal = Calendar.getInstance();
                System.out.println("cal: " + formatInputIFL(cal));
                long one_minute = createKeyTimestamp(cal, TYPE_TIME.ONE_MINUTE);
                String s = timestamp2StringIFL(one_minute);
                System.out.println("one_minute: " + one_minute + " <==> s: " + s);
                Thread.sleep(3000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void test1(){
        try {
            Calendar cal = Calendar.getInstance();
            String s = formatInputIFL(cal);
            System.out.println("s: " + s);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void test2(){
        try {
            for (TYPE_TIME tt : TYPE_TIME.values()) {
                System.out.println(tt.getName() + "\t\t <==> " + tt.getOtherName());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
