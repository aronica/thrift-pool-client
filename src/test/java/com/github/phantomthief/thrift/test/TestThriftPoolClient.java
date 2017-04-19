/**
 *
 */
package com.github.phantomthief.thrift.test;

import com.github.phantomthief.thrift.client.pool.ThriftServerInfo;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author w.vela
 */
public class TestThriftPoolClient {

    private final Logger logger = getLogger(getClass());

    @Test
    public void testEcho() throws InterruptedException {

        // define serverList provider, you can use dynamic provider here to impl on the fly changing...
        Supplier<List<ThriftServerInfo>> serverListProvider = () -> Arrays.asList( //
                ThriftServerInfo.of("127.0.0.1", 9092), //
                ThriftServerInfo.of("127.0.0.1", 9091), //
                ThriftServerInfo.of("127.0.0.1", 9090));

        // init pool client
//        ThriftClientImpl client = new ThriftClientImpl(serverListProvider);
//
//        ExecutorService executorService = Executors.newFixedThreadPool(10);
//
//        for (int i = 0; i < 100; i++) {
//            int counter = i;
//            executorService.submit(() -> {
//                try {
//                    String result = client.iface(Client.class).echo("hi " + counter + "!");
//                    logger.info("get result: {}", result);
//                } catch (Throwable e) {
//                    logger.error("get client fail", e);
//                }
//            });
//        }
//
//        MoreExecutors.shutdownAndAwaitTermination(executorService, 1, MINUTES);
    }
}
