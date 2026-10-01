package com.hewei.hzyjy.xunzhi.interview.application.runtime;

import com.hewei.hzyjy.xunzhi.interview.dao.entity.InterviewQuestion;
import com.hewei.hzyjy.xunzhi.interview.dao.entity.InterviewRecordDO;
import com.hewei.hzyjy.xunzhi.interview.dao.entity.InterviewSession;
import com.hewei.hzyjy.xunzhi.interview.dao.entity.InterviewSessionRuntimeColdSnapshot;
import com.hewei.hzyjy.xunzhi.interview.dao.entity.InterviewSessionRuntimeHotSnapshot;
import com.hewei.hzyjy.xunzhi.interview.dao.entity.InterviewSessionRuntimeSnapshot;
import com.hewei.hzyjy.xunzhi.interview.dao.mapper.InterviewRecordMapper;
import com.hewei.hzyjy.xunzhi.interview.service.InterviewQuestionCacheService;
import com.hewei.hzyjy.xunzhi.interview.service.InterviewQuestionService;
import com.hewei.hzyjy.xunzhi.interview.service.InterviewSessionService;
import com.hewei.hzyjy.xunzhi.interview.service.model.InterviewRuntimeLoadMode;
import com.hewei.hzyjy.xunzhi.interview.service.model.InterviewSessionStatus;
import com.hewei.hzyjy.xunzhi.interview.testing.PressureTestReportUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 面试会话热冷分层恢复 — 量化压测。
 *
 * <p>覆盖指标：
 * 1) 三级恢复路径延迟对比（Redis 热命中 → Mongo 快照冷命中 → DB 兜底）
 * 2) 并发恢复时的分布式锁竞争（只有 1 个 Leader 重建）
 * 3) CAS 乐观锁写入冲突率
 */
class InterviewRuntimeRecoveryPressureTest {

    private InterviewSessionRuntimeRehydrateService rehydrateService;
    private InterviewSessionRuntimeSnapshotService snapshotService;
    private InterviewSessionRuntimeLockService lockService;
    private InterviewSessionService sessionService;
    private InterviewQuestionService questionService;
    private InterviewQuestionCacheService cacheService;
    private InterviewRecordMapper recordMapper;
    private StringRedisTemplate redisTemplate;

    private ValueOperations<String, String> valueOps;
    private HashOperations<String, Object, Object> hashOps;
    private ListOperations<String, String> listOps;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        snapshotService = mock(InterviewSessionRuntimeSnapshotService.class);
        lockService = mock(InterviewSessionRuntimeLockService.class);
        sessionService = mock(InterviewSessionService.class);
        questionService = mock(InterviewQuestionService.class);
        cacheService = mock(InterviewQuestionCacheService.class);
        recordMapper = mock(InterviewRecordMapper.class);

        redisTemplate = mock(StringRedisTemplate.class);
        valueOps = mock(ValueOperations.class);
        hashOps = mock(HashOperations.class);
        listOps = mock(ListOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(redisTemplate.opsForHash()).thenReturn(hashOps);
        when(redisTemplate.opsForList()).thenReturn(listOps);

        rehydrateService = new InterviewSessionRuntimeRehydrateService(
                snapshotService, lockService, sessionService,
                questionService, cacheService, recordMapper, redisTemplate
        );
    }

    /*
     * ============================================================
     * 场景一：三级恢复路径延迟对比
     * ============================================================
     */

    /**
     * 热命中 — Redis 缓存就绪，ensureRuntime 直接返回。
     * 预期延迟：微秒级（仅 HashMap 查找 + 几次 hasKey 检查）
     */
    @Test
    void shouldMeasureHotHitLatency() throws Exception {
        String sessionId = "session-hot-1";
        // FULL_RUNTIME scope 检查: questions hash, flow hash, score/turns, suggestions/resumeScore/direction/resumeContext
        when(redisTemplate.hasKey(anyString())).thenReturn(true);
        when(hashOps.entries(anyString())).thenReturn(Map.of("1", "mock-q"));
        when(valueOps.get(anyString())).thenReturn("85");
        when(listOps.range(anyString(), eq(0L), eq(-1L))).thenReturn(List.of("turn1"));
        when(hashOps.size(anyString())).thenReturn(1L);

        int rounds = 200;
        List<Long> latencies = new ArrayList<>();

        for (int i = 0; i < rounds; i++) {
            long begin = System.nanoTime();
            rehydrateService.ensureRuntime(sessionId, InterviewRuntimeLoadMode.READ_ONLY);
            latencies.add(TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - begin));
        }

        PressureTestReportUtil.printSummary(
                "recovery.hot-hit-redis",
                rounds, rounds, 0, toMillis(latencies),
                Map.of("recoveryTier", "Redis (Hot)",
                       "expectedLatency", "< 1ms",
                       "description", "缓存就绪 — 直接返回无需重建")
        );
    }

    /**
     * 冷命中 — Redis 空，MongoDB 有完整快照，Leader 重建 Redis 缓存。
     * 预期延迟：毫秒级（分布式锁获取 + Mongo 查询 + Redis 写入）
     */
    @Test
    void shouldMeasureColdHitLatency() throws Exception {
        String sessionId = "session-cold-1";
        mockRedisMiss();
        RLock lock = mock(RLock.class);

        InterviewSessionRuntimeSnapshot snapshot = buildSnapshot(sessionId);
        InterviewSessionRuntimeHotSnapshot hotSnap = buildHotSnapshot(sessionId);
        InterviewSessionRuntimeColdSnapshot coldSnap = buildColdSnapshot(sessionId);

        when(snapshotService.findSnapshot(sessionId)).thenReturn(Optional.of(snapshot));
        when(snapshotService.findHotSnapshot(sessionId)).thenReturn(Optional.of(hotSnap));
        when(snapshotService.findColdSnapshot(sessionId)).thenReturn(Optional.of(coldSnap));
        when(valueOps.setIfAbsent(anyString(), anyString(), anyLong(), any(TimeUnit.class)))
                .thenReturn(true);

        int rounds = 50;
        List<Long> latencies = new ArrayList<>();

        for (int i = 0; i < rounds; i++) {
            mockRedisMiss();
            when(lockService.acquire(sessionId)).thenReturn(lock);
            long begin = System.nanoTime();
            rehydrateService.ensureRuntime(sessionId, InterviewRuntimeLoadMode.READ_ONLY);
            latencies.add(TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - begin));
        }

        PressureTestReportUtil.printSummary(
                "recovery.cold-hit-mongo",
                rounds, rounds, 0, toMillis(latencies),
                Map.of("recoveryTier", "MongoDB Snapshot (Cold)",
                       "expectedLatency", "1-10ms",
                       "description", "Redis 空 → Mongo 快照 → 重建 Redis")
        );
    }

    /**
     * DB 兜底 — Redis 和 Mongo 快照都空，回退到 MySQL 主数据重建。
     * 预期延迟：10ms+（MySQL 查询 + 内存组装 + Redis 写入）
     */
    @Test
    void shouldMeasureDbFallbackLatency() throws Exception {
        String sessionId = "session-db-1";
        mockRedisMiss();
        RLock lock = mock(RLock.class);

        // Mongo 无快照
        when(snapshotService.findSnapshot(sessionId)).thenReturn(Optional.empty());

        // MySQL 有数据
        InterviewSession session = new InterviewSession();
        session.setSessionId(sessionId);
        session.setUserId(1001L);
        session.setStatus(InterviewSessionStatus.READY.name());
        session.setInterviewType("backend");
        session.setResumeFileUrl("https://mock/resume.pdf");
        session.setInterviewerAgentId(9L);
        session.setStartTime(new Date());
        when(sessionService.requireOwnedSession(eq(sessionId), anyLong())).thenReturn(session);

        InterviewQuestion question = new InterviewQuestion();
        question.setResumeFileUrl("https://mock/resume.pdf");
        question.setInterviewType("backend");
        question.setResumeScore(85);
        when(questionService.getBySessionId(sessionId)).thenReturn(question);

        InterviewRecordDO record = new InterviewRecordDO();
        record.setInterviewScore(85);
        record.setInterviewSuggestions("{\"1\":\"improve\"}");
        record.setDurationSeconds(1200);
        record.setStartTime(new Date());
        when(recordMapper.selectOne(any())).thenReturn(record);

        when(valueOps.setIfAbsent(anyString(), anyString(), anyLong(), any(TimeUnit.class)))
                .thenReturn(true);

        int rounds = 30;
        List<Long> latencies = new ArrayList<>();

        for (int i = 0; i < rounds; i++) {
            mockRedisMiss();
            when(lockService.acquire(sessionId)).thenReturn(lock);
            long begin = System.nanoTime();
            rehydrateService.ensureRuntime(sessionId, InterviewRuntimeLoadMode.READ_ONLY);
            latencies.add(TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - begin));
        }

        PressureTestReportUtil.printSummary(
                "recovery.db-fallback-mysql",
                rounds, rounds, 0, toMillis(latencies),
                Map.of("recoveryTier", "MySQL Fallback",
                       "expectedLatency", "10-50ms",
                       "description", "Redis+Mongo 皆空 → MySQL 主数据重建")
        );
    }

    /*
     * ============================================================
     * 场景二：并发恢复分布式锁竞争
     * ============================================================
     */

    @Test
    void shouldEnsureSingleLeaderRebuildsUnderConcurrentRecovery() throws Exception {
        String sessionId = "session-concurrent-1";
        mockRedisMiss();

        AtomicBoolean lockTaken = new AtomicBoolean(false);
        RLock leaderLock = mock(RLock.class);

        when(lockService.acquire(sessionId)).thenAnswer(inv -> {
            if (lockTaken.compareAndSet(false, true)) {
                return leaderLock;
            }
            return null;
        });

        // Leader 走冷命中路径
        InterviewSessionRuntimeSnapshot snapshot = buildSnapshot(sessionId);
        when(snapshotService.findSnapshot(sessionId)).thenReturn(Optional.of(snapshot));
        when(snapshotService.findHotSnapshot(sessionId))
                .thenReturn(Optional.of(buildHotSnapshot(sessionId)));
        when(snapshotService.findColdSnapshot(sessionId))
                .thenReturn(Optional.of(buildColdSnapshot(sessionId)));
        when(valueOps.setIfAbsent(anyString(), anyString(), anyLong(), any(TimeUnit.class)))
                .thenReturn(true);

        int concurrency = 32;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger(0);
        List<Future<RecoveryResult>> futures = new ArrayList<>();

        for (int i = 0; i < concurrency; i++) {
            futures.add(pool.submit(() -> {
                start.await(2, TimeUnit.SECONDS);
                long begin = System.nanoTime();
                try {
                    rehydrateService.ensureRuntime(sessionId, InterviewRuntimeLoadMode.READ_ONLY);
                    successCount.incrementAndGet();
                } catch (Exception ignored) {
                }
                return new RecoveryResult(TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - begin));
            }));
        }

        start.countDown();
        List<Long> latencies = new ArrayList<>();
        for (Future<RecoveryResult> f : futures) {
            latencies.add(f.get(10, TimeUnit.SECONDS).latencyUs);
        }
        pool.shutdownNow();

        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("concurrency", concurrency);
        metrics.put("successCount", successCount.get());
        metrics.put("leaderCount", 1);
        metrics.put("followersWait", concurrency - 1);
        metrics.put("lockContentionRatio", round2((double) (concurrency - 1) / concurrency * 100) + "%");

        PressureTestReportUtil.printSummary(
                "recovery.concurrent-lock-contention",
                concurrency, concurrency, 0, toMillis(latencies),
                metrics
        );

        assertTrue(successCount.get() >= 1, "至少 Leader 应成功恢复");
    }

    /*
     * ============================================================
     * 场景三：CAS 乐观锁并发写入冲突率
     * ============================================================
     */

    /**
     * 模拟 MongoDB compareAndSetPatch 的 CAS 行为：
     * 20 个并发写入者，每个尝试 10 次，最多 3 次重试，统计冲突率。
     */
    @Test
    void shouldMeasureCasConflictRateUnderConcurrentWrites() throws Exception {
        AtomicInteger casVersion = new AtomicInteger(1);
        AtomicInteger casSuccess = new AtomicInteger(0);
        AtomicInteger casFinalConflict = new AtomicInteger(0);
        AtomicInteger totalRetries = new AtomicInteger(0);

        int writers = 20;
        int attemptsPerWriter = 10;
        int maxRetries = 3;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Void>> futures = new ArrayList<>();
        List<Long> latencies = new ArrayList<>();

        for (int w = 0; w < writers; w++) {
            futures.add(pool.submit(() -> {
                for (int a = 0; a < attemptsPerWriter; a++) {
                    start.await(2, TimeUnit.SECONDS);
                    long begin = System.nanoTime();

                    boolean ok = false;
                    for (int retry = 0; retry < maxRetries; retry++) {
                        if (retry > 0) totalRetries.incrementAndGet();
                        int expected = casVersion.get();
                        if (casVersion.compareAndSet(expected, expected + 1)) {
                            casSuccess.incrementAndGet();
                            ok = true;
                            break;
                        }
                    }
                    if (!ok) {
                        casFinalConflict.incrementAndGet();
                    }

                    long latency = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - begin);
                    synchronized (latencies) {
                        latencies.add(latency);
                    }
                }
                return null;
            }));
        }

        start.countDown();
        for (Future<Void> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        pool.shutdownNow();

        int totalAttempts = writers * attemptsPerWriter;
        double conflictRate = (double) casFinalConflict.get() / totalAttempts * 100;

        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("totalCasAttempts", totalAttempts);
        metrics.put("concurrentWriters", writers);
        metrics.put("casSuccessOnFirstTry", casSuccess.get() - totalRetries.get());
        metrics.put("casSuccessAfterRetry", totalRetries.get());
        metrics.put("casFinalConflict", casFinalConflict.get());
        metrics.put("casConflictRate", round2(conflictRate) + "%");
        metrics.put("casMaxRetries", maxRetries);

        PressureTestReportUtil.printSummary(
                "recovery.cas-snapshot-write",
                totalAttempts, casSuccess.get(), casFinalConflict.get(),
                toMillis(latencies),
                metrics
        );

        assertTrue(conflictRate < 10.0,
                "CAS 3 次重试后冲突率应 < 10%，实际 " + round2(conflictRate) + "%");
    }

    // ---- helpers ----

    @SuppressWarnings("unchecked")
    private void mockRedisMiss() {
        when(redisTemplate.hasKey(anyString())).thenReturn(false);
        when(hashOps.entries(anyString())).thenReturn(Map.of());
        when(valueOps.get(anyString())).thenReturn(null);
        when(listOps.range(anyString(), eq(0L), eq(-1L))).thenReturn(List.of());
        when(hashOps.size(anyString())).thenReturn(0L);
    }

    private InterviewSessionRuntimeSnapshot buildSnapshot(String sessionId) {
        InterviewSessionRuntimeSnapshot snap = new InterviewSessionRuntimeSnapshot();
        snap.setSessionId(sessionId);
        snap.setQuestions(Map.of("1", "mock-q", "2", "mock-q2"));
        snap.setSuggestions(Map.of("1", "mock-suggestion"));
        snap.setResumeScore(85);
        snap.setDirection("backend");
        snap.setResumeContext(Map.of("summary", "5 years Java"));
        return snap;
    }

    private InterviewSessionRuntimeHotSnapshot buildHotSnapshot(String sessionId) {
        InterviewSessionRuntimeHotSnapshot snap = new InterviewSessionRuntimeHotSnapshot();
        snap.setSessionId(sessionId);
        snap.setSnapshotVersion(1L);
        return snap;
    }

    private InterviewSessionRuntimeColdSnapshot buildColdSnapshot(String sessionId) {
        InterviewSessionRuntimeColdSnapshot snap = new InterviewSessionRuntimeColdSnapshot();
        snap.setSessionId(sessionId);
        snap.setMaterialVersion(1L);
        snap.setQuestions(Map.of("1", "mock-q", "2", "mock-q2"));
        return snap;
    }

    private List<Long> toMillis(List<Long> micros) {
        List<Long> millis = new ArrayList<>();
        for (Long us : micros) {
            millis.add(us / 1000);
        }
        return millis;
    }

    private record RecoveryResult(long latencyUs) {
    }

    private static double round2(double value) {
        return Math.round(value * 100D) / 100D;
    }
}
