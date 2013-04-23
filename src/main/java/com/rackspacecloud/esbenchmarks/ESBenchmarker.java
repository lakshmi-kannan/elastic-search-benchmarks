package com.rackspacecloud.esbenchmarks;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.spi.LoggerFactory;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.*;

public class ESBenchmarker {
    private static final Integer ES_PORT=9300;
    private static final String ES_HOST="10.20.76.50";
    private static final Integer NUMBER_OF_THREADS=5;
    private static final CountDownLatch latch = new CountDownLatch(NUMBER_OF_THREADS);
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(ESBenchmarker.class);

    private static Settings settings = ImmutableSettings.settingsBuilder()
          //   .put("client.transport.sniff", true)
             .put("client.transport.ignore_cluster_name", true)
             .put("client.transport.ping_timeout", "10s")
             .put("discovery.zen.unicast.hosts", "10.20.76.50:9300,10.20.76.50:9300")
             .build();
    private static Client client = null;

    public static void main(String[] args) throws Exception {
        client = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(ES_HOST, ES_PORT))
                .addTransportAddress(new InetSocketTransportAddress(ES_HOST, ES_PORT));
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(10);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 5, 10000, TimeUnit.DAYS, queue);

        log.info("Benchmarking with Number of writers: " + NUMBER_OF_THREADS);
        for (int i = 0; i < NUMBER_OF_THREADS; i++) {
            executor.execute(new ESIndexer());
        }

        log.info("Waiting for benchmark threads to finish.");
        latch.await();
        log.info("Done benchmarking.");
        System.exit(0);
    }

    private static class ESIndexer implements Runnable {
        private static final int ITERATIONS = 1000000;

        public void run() {
            double sum = 0;
            int failures = 0;
            for (int count = 0; count < ITERATIONS; count++) {
                try {
                    long start = System.currentTimeMillis();
                    final String accountId = "ac" + UUID.randomUUID();
                    final XContentBuilder content = generateFakeData(accountId);
                    IndexResponse response = client.prepareIndex("ele-bf", "metricsIndex-" + getIndex(accountId))
                            .setSource(content)
                            .execute()
                            .actionGet();
                    long stop = System.currentTimeMillis();
                    sum += (stop - start);
                } catch (IOException ex) {
                    failures++;
                }
            }

            log.info("Average index latency (ms):" + sum/(ITERATIONS - failures));
            log.info("Failures (count):" + failures);
            latch.countDown();
        }

        private XContentBuilder generateFakeData(String accountId) throws IOException {
            XContentBuilder builder = jsonBuilder()
                    .startObject()
                        .field("accountId", accountId)
                        .field("metricName", "ord1-" + UUID.randomUUID())
                    .endObject();

            return builder;
        }

        private String getIndex(String accountId) {
            return String.valueOf(Long.parseLong(DigestUtils.md5Hex(accountId).substring(30), 16) % 128);
        }
    }
}
