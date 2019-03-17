package com.bigdata.etl.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class UDFZodiac extends UDF {
    private SimpleDateFormat sdf;

    public UDFZodiac() {
        sdf = new SimpleDateFormat("yyyy-MM-dd");
    }

    public String evaluate(Calendar calendar) {
        return this.evaluate(calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH));
    }

    /**
     * SimpleDateFormat解析String为Date类型，通过Calendar得到具体的month和day
     *
     */
    public String evaluate(String dateStr) {
        Date date = null;
        try {
            date = sdf.parse(dateStr);
        } catch (ParseException e) {
            return null;
        }

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);

        return this.evaluate(calendar);
    }

    public String evaluate(Integer month, Integer day) {
        if(month > 12 || month <1 || day < 1 || day > 31){
            throw new RuntimeException("input is invalid!");
        }
        /**
         *  思路：如果某一月的日期小于等于该月的边界日期，直接去month-1的下标；如果大于边界日期，去month的下标。
         */
        String[] constellations = {"魔羯座" ,"水瓶座", "双鱼座", "牡羊座", "金牛座", "双子座", "巨蟹座", "狮子座", "处女座", "天秤座","天蝎座", "射手座", "魔羯座" };
        final int[] constellationEdgeDay = { 20,18,20,20,20,21,22,22,22,22,21,21};
        if (day <= constellationEdgeDay[month-1]) {
            month = month - 1;
        }
        month = month % 12;
        return constellations[month];
    }

}
