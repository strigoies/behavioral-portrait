package com.yisa.utils;

import java.util.Calendar;

public class CaptureTime {

    public long yesterdayStartTimestamp;

    public long yesterdayEndTimestamp;

    public CaptureTime(){
        Calendar yesterdayCalendar = Calendar.getInstance();
        yesterdayCalendar.set(yesterdayCalendar.get(Calendar.YEAR),yesterdayCalendar.get(Calendar.MONTH),yesterdayCalendar.get(Calendar.DAY_OF_MONTH)-10,0,0,0);
        this.yesterdayStartTimestamp = yesterdayCalendar.getTime().getTime()/1000;

        Calendar todayCalendar = Calendar.getInstance();
        todayCalendar.set(todayCalendar.get(Calendar.YEAR),todayCalendar.get(Calendar.MONTH),todayCalendar.get(Calendar.DAY_OF_MONTH)-1,23,59,59);
        this.yesterdayEndTimestamp = todayCalendar.getTime().getTime()/1000;
    }
}
