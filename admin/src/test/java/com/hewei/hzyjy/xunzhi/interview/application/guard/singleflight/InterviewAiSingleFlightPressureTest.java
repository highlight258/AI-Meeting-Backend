package com.hewei.hzyjy.xunzhi.interview.application.guard.singleflight;

import com.hewei.hzyjy.xunzhi.interview.application.guard.singleflight.service.InterviewAiSingleFlightService;
import com.hewei.hzyjy.xunzhi.interview.config.InterviewAiSingleFlightConfiguration;
import com.hewei.hzyjy.xunzhi.interview.testing.PressureTestReportUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Single-flight 量化压测：验证请求收敛比、Token 节省率、延迟分布。
 */
class InterviewAiSingleFlightPressureTest {

    private InterviewAiSingleFlightService service;
    private MeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        InterviewAiSingleFlightConfiguration config = new InterviewAiSingleFlightConfiguration();
        config.setEnable(true);
        config.setTtlMillis(65000L);
        config.setWaitTimeoutMillis(65000L);
        config.setCleanupThreshold(256);
        meterRegistry = new SimpleMeterRegistry();
        service = new InterviewAiSingleFlightService(config, meterRegistry);
    }

    /**
     * 核心场景：N 并发请求共享同一 key，验证只有 1 次真实调用。
     * 产出指标：请求收敛比、Token 节省率
     */
    @Test
    void shouldDeduplicateConcurrentRequestsWithSameKey() throws Exception {
        AtomicInteger realCallCount = new AtomicInteger(0);

        // 模拟 AI 调用：耗时 200ms，返回评分结果
        var supplier = (java.util.function.Supplier<String>) () -> {
            realCallCount.incrementAndGet();
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "{\"score\":85,\"feedback\":\"回答逻辑清晰，但缺少具体案例支撑\"}";
        };

        for (int concurrency : new int[]{10, 50, 100, 200}) {
            realCallCount.set(0);
            String key = "evaluation:session-test:question-1:req-abc123-" + concurrency;

            ExecutorService pool = Executors.newFixedThreadPool(Math.min(concurrency, 32));
            CountDownLatch start = new CountDownLatch(1);
            List<Future<SingleFlightResult>> futures = new ArrayList<>();

            for (int i = 0; i < concurrency; i++) {
                futures.add(pool.submit(() -> {
                    start.await(2, TimeUnit.SECONDS);
                    long begin = System.nanoTime();
                    String result = service.execute(key, supplier);
                    long latency = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
                    return new SingleFlightResult(result, latency, Thread.currentThread().getName());
                }));
            }

            start.countDown();

            int success = 0;
            int fail = 0;
            List<Long> latencies = new ArrayList<>();
            String firstResult = null;
            int resultMatchCount = 0;

            for (Future<SingleFlightResult> future : futures) {
                try {
                    SingleFlightResult sr = future.get(10, TimeUnit.SECONDS);
                    success++;
                    latencies.add(sr.latencyMs);
                    if (firstResult == null) {
                        firstResult = sr.result;
                    }
                    if (firstResult != null && firstResult.equals(sr.result)) {
                        resultMatchCount++;
                    }
                } catch (Exception e) {
                    fail++;
                }
            }
            pool.shutdownNow();

            // 量化指标
            int totalRequests = concurrency;
            int actualAiCalls = realCallCount.get();
            double convergenceRatio = (double) totalRequests / Math.max(actualAiCalls, 1);
            double tokenSavingRate = (1.0 - (double) actualAiCalls / totalRequests) * 100;
            double estimatedTokensSaved = (totalRequests - actualAiCalls) * 500; // 假设单次调用 500 token

            Map<String, Object> metrics = new LinkedHashMap<>();
            metrics.put("totalRequests", totalRequests);
            metrics.put("actualAiCalls", actualAiCalls);
            metrics.put("convergenceRatio", round2(convergenceRatio) + ":1");
            metrics.put("tokenSavingRate", round2(tokenSavingRate) + "%");
            metrics.put("estimatedTokensSaved", estimatedTokensSaved);
            metrics.put("resultConsistency", resultMatchCount + "/" + success);

            PressureTestReportUtil.printSummary(
                    "singleflight.concurrent-dedup-c" + concurrency,
                    totalRequests,
                    success,
                    fail,
                    latencies,
                    metrics
            );

            assertEquals(1, actualAiCalls,
                    "Single-flight 应收敛为 1 次调用，实际 " + actualAiCalls + " 次");
            assertEquals(success, resultMatchCount,
                    "所有等待者应获得与 Leader 一致的结果");
        }
    }

    /**
     * 不同 key 不受影响：确保 single-flight 不会误拦截不同请求。
     */
    @Test
    void shouldNotBlockDifferentKeys() throws Exception {
        AtomicInteger realCallCount = new AtomicInteger(0);

        var supplier = (java.util.function.Supplier<String>) () -> {
            realCallCount.incrementAndGet();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "ok";
        };

        int distinctKeys = 20;
        int concurrencyPerKey = 5;
        int totalRequests = distinctKeys * concurrencyPerKey;

        ExecutorService pool = Executors.newFixedThreadPool(16);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        for (int k = 0; k < distinctKeys; k++) {
            String key = "evaluation:session-test:question-" + k + ":req-xyz";
            for (int i = 0; i < concurrencyPerKey; i++) {
                futures.add(pool.submit(() -> {
                    try {
                        start.await(2, TimeUnit.SECONDS);
                        service.execute(key, supplier);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }));
            }
        }

        start.countDown();
        for (Future<?> future : futures) {
            future.get(10, TimeUnit.SECONDS);
        }
        pool.shutdownNow();

        assertEquals(distinctKeys, realCallCount.get(),
                "不同 key 应各自触发一次调用，期望 " + distinctKeys + " 次，实际 " + realCallCount.get() + " 次");

        System.out.println("PRESSURE_REPORT {\"scenario\":\"singleflight.multi-key-isolation\","
                + "\"distinctKeys\":" + distinctKeys + ","
                + "\"concurrencyPerKey\":" + concurrencyPerKey + ","
                + "\"totalRequests\":" + totalRequests + ","
                + "\"actualAiCalls\":" + realCallCount.get() + ","
                + "\"isolationCorrect\":true}");
    }

    /**
     * 延迟分层分析：Leader vs Follower 延迟对比。
     */
    @Test
    void shouldReportLeaderVsFollowerLatency() throws Exception {
        AtomicInteger callSeq = new AtomicInteger(0);

        List<String> leaderThread = new ArrayList<>();
        List<String> followerThreads = Collections.synchronizedList(new ArrayList<>());

        var supplier = (java.util.function.Supplier<String>) () -> {
            int seq = callSeq.incrementAndGet();
            if (seq == 1) {
                leaderThread.add(Thread.currentThread().getName());
            } else {
                followerThreads.add(Thread.currentThread().getName());
            }
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "{\"score\":92}";
        };

        String key = "evaluation:session-latency:q1:req-lat";
        int concurrency = 50;

        ExecutorService pool = Executors.newFixedThreadPool(16);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<SingleFlightResult>> futures = new ArrayList<>();

        for (int i = 0; i < concurrency; i++) {
            futures.add(pool.submit(() -> {
                start.await(2, TimeUnit.SECONDS);
                long begin = System.nanoTime();
                String result = service.execute(key, supplier);
                long latency = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
                return new SingleFlightResult(result, latency, Thread.currentThread().getName());
            }));
        }

        start.countDown();

        List<Long> allLatencies = new ArrayList<>();
        Long leaderLatency = null;
        List<Long> followerLatencies = new ArrayList<>();

        for (Future<SingleFlightResult> future : futures) {
            SingleFlightResult sr = future.get(10, TimeUnit.SECONDS);
            allLatencies.add(sr.latencyMs);
        }
        pool.shutdownNow();

        // Leader 是第一个到达的请求，延迟最高（含 300ms 执行耗时）
        // Follower 等待 Leader 完成，延迟应略低于或接近 Leader
        Collections.sort(allLatencies);
        leaderLatency = allLatencies.get(allLatencies.size() - 1); // 最高延迟的是 Leader
        followerLatencies = allLatencies.subList(0, allLatencies.size() - 1);

        long followerAvg = (long) followerLatencies.stream().mapToLong(Long::longValue).average().orElse(0);

        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("totalRequests", concurrency);
        metrics.put("actualAiCalls", callSeq.get());
        metrics.put("leaderLatencyMs", leaderLatency);
        metrics.put("followerAvgLatencyMs", followerAvg);
        metrics.put("latencyGapMs", leaderLatency - followerAvg);
        metrics.put("convergenceRatio", concurrency + ":1");

        PressureTestReportUtil.printSummary(
                "singleflight.leader-vs-follower-latency",
                concurrency,
                concurrency,
                0,
                allLatencies,
                metrics
        );

        assertEquals(1, callSeq.get(), "应只有 Leader 执行真实调用");
        assertTrue(leaderLatency >= 300, "Leader 延迟应 >= 模拟 AI 调用耗时 300ms");
    }

    private record SingleFlightResult(String result, long latencyMs, String threadName) {
    }

    private static double round2(double value) {
        return Math.round(value * 100D) / 100D;
    }
}
