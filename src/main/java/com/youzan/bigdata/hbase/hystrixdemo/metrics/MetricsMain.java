package com.youzan.bigdata.hbase.hystrixdemo.metrics;

import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandMetrics;

import java.io.IOException;
import java.lang.reflect.Method;

public class MetricsMain {

    private static String getStatsStringFromMetrics(HystrixCommandMetrics metrics) {
        StringBuilder m = new StringBuilder();
        if (metrics != null) {
            HystrixCommandMetrics.HealthCounts health = metrics.getHealthCounts();
            m.append("Requests: ").append(health.getTotalRequests()).append(" ");
            m.append("Errors: ").append(health.getErrorCount()).append(" (").append(health.getErrorPercentage()).append("%)   ");
            m.append("Mean: ").append(metrics.getExecutionTimePercentile(50)).append(" ");
            m.append("75th: ").append(metrics.getExecutionTimePercentile(75)).append(" ");
            m.append("90th: ").append(metrics.getExecutionTimePercentile(90)).append(" ");
            m.append("99th: ").append(metrics.getExecutionTimePercentile(99)).append(" ");
        }
        return m.toString();
    }

    public static void main(String[] args) {

        Thread checker = new Thread(new Runnable() {
            public void run() {
                while (true) {
                    HystrixCommandMetrics metrics = HystrixCommandMetrics.getInstance(HystrixCommandKey.Factory.asKey(RandomCommand.class.getSimpleName()));
                    System.out.println("metrics:" + (  metrics == null ? "not initialized" : getStatsStringFromMetrics(metrics)));

//                    try {
//                        Class<?> clazz = Class.forName(HystrixCommandMetrics.class.getName());
//                        Method resetMethod = clazz.getDeclaredMethod("reset", null);
//                        resetMethod.setAccessible(true);
//                        resetMethod.invoke(null);
//                    } catch (Throwable t) {
//                        System.out.println(t.toString());
//                        t.printStackTrace();
//                    }
                    try {
                        Thread.sleep(1000);
                    } catch (Exception ioe) {
                        // ignore
                    } finally {

                    }

                }
            }
        });
        checker.setDaemon(true);
        checker.start();

        for (int i = 0; i < 1000; i++) {
            new RandomCommand().execute();
            try {
                Thread.sleep(10);
            } catch (Exception ioe) {
                // ignore
            }
        }


    }
}
