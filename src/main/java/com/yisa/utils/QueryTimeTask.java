package com.yisa.utils;

import java.util.Date;
import java.util.TimerTask;

public class QueryTimeTask extends TimerTask {
    private String taskName;

    public QueryTimeTask(String taskName) {
        this.taskName = taskName;
    }

    @Override
    public void run() {
        System.out.println(new Date() + " : 任务「" + taskName + "」被执行。");
    }
}

