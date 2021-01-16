package com.github.mvh77.ktc;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

public class CompletableFuturesTest {

    @Test
    public void testSequence() throws ExecutionException, InterruptedException {
        var seq = CompletableFutures.sequence(List.of(completedFuture("foo")), Collectors.toList()).get();
        Assert.assertEquals("foo", seq.get(0));

        var fseq = CompletableFutures.sequence(List.of(completedFuture("foo"), failedFuture(new RuntimeException("bar"))), Collectors.toList());
        Assert.assertTrue(fseq.isCompletedExceptionally());
    }

    @Test
    public void testCombined() throws ExecutionException, InterruptedException {
        var seq = CompletableFutures.combined(List.of(completedFuture("foo")), Collectors.toList()).get();
        Assert.assertEquals("foo", seq.get(0));

        var latch = new CountDownLatch(1);
        CompletableFutures.combined(List.of(completedFuture("foo"), failedFuture(new RuntimeException("bar")), failedFuture(new RuntimeException("baz"))), Collectors.toList())
                .whenComplete((s, th) -> {
                    Assert.assertTrue(th instanceof CompletableFutures.AccumulatedThrowable);
                    var ex = ((CompletableFutures.AccumulatedThrowable)th).getAccumulatedExceptions();
                    Assert.assertEquals(2, ex.size());
                    latch.countDown();
                });
        latch.await();
    }
}
