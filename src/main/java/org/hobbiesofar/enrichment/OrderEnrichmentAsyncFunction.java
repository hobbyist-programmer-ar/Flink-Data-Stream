package org.hobbiesofar.enrichment;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.hobbiesofar.dto.EnrichedOrder;
import org.hobbiesofar.dto.Order;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OrderEnrichmentAsyncFunction extends RichAsyncFunction<Order, EnrichedOrder> {

    private transient ExecutorService executor;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        executor = Executors.newFixedThreadPool(20);
    }

    @Override
    public void close() {
        executor.shutdown();
    }


    @Override
    public void asyncInvoke(Order order, ResultFuture<EnrichedOrder> resultFuture) throws Exception {

    }
}
