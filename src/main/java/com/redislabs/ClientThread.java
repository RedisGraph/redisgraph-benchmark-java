package com.redislabs;

import com.google.common.util.concurrent.RateLimiter;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.graph.ResultSet;
import org.HdrHistogram.*;

public class ClientThread extends Thread {
    private final int requests;
    private final UnifiedJedis rg;
    private final String query;
    private final String key;
    private final Histogram histogram;
    private final Histogram graphInternalHistogram;
    private final RateLimiter rateLimiter;

    ClientThread(UnifiedJedis rg, Integer requests, String key, String query, ConcurrentHistogram histogram, ConcurrentHistogram graphInternalHistogram) {
        super("Client thread");
        this.requests = requests;
        this.rg = rg;
        this.query = query;
        this.key = key;
        this.histogram = histogram;
        this.graphInternalHistogram = graphInternalHistogram;
        this.rateLimiter = null;
    }

    ClientThread(UnifiedJedis rg, Integer requests, String key, String query, ConcurrentHistogram histogram, ConcurrentHistogram graphInternalHistogram, RateLimiter perClientRateLimiter) {
        super("Client thread");
        this.requests = requests;
        this.rg = rg;
        this.query = query;
        this.key = key;
        this.histogram = histogram;
        this.graphInternalHistogram = graphInternalHistogram;
        this.rateLimiter = perClientRateLimiter;
    }

    public void run() {
        for (int i = 0; i < requests; i++) {
            if (rateLimiter!=null){
                // blocks the executing thread until a permit is available.
                rateLimiter.acquire(1);
            }
            long startTime = System.nanoTime();
            ResultSet resultSet = rg.graphQuery(key, query);
            long durationMicros = (System.nanoTime() - startTime) / 1000;
            String splitted = resultSet.getStatistics().queryIntervalExecutionTime().split(" ")[0];
            double internalDuration = Double.parseDouble(splitted) * 1000;
            graphInternalHistogram.recordValue((long) internalDuration);
            histogram.recordValue(durationMicros);
        }
    }
}
