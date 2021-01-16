package com.github.mvh77.ktc;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class CompletableFutures {

    public static <T, A, I extends Iterable<? extends T>> CompletableFuture<I> sequence(Collection<CompletableFuture<T>> collection,
                                                                                        Collector<? super T, A, I> collector) {
        var a = collector.supplier().get();
        var result = new CompletableFuture<I>();
        if (collection.isEmpty()) {
            result.completeAsync(() -> collector.finisher().apply(a));
        } else {
            var count = new AtomicInteger(collection.size());
            var accumulator = concurrentAcc(collector);
            collection.forEach(cf -> cf.whenComplete((t, th) -> {
                if (t != null && count.get() > 0) {
                    accumulator.accept(a, t);
                    if (count.decrementAndGet() == 0) {
                        // completes if not already failed
                        result.completeAsync(() -> collector.finisher().apply(a));
                    }
                }
                if (th != null) {
                    // fail on first exception
                    result.completeExceptionally(th);
                    count.set(-1);
                }
            }));
        }
        return result;
    }

    public static <T, A, I extends Iterable<? extends T>> CompletableFuture<I> combined(Collection<CompletableFuture<T>> collection,
                                                                                        Collector<? super T, A, I> collector) {
        var a = collector.supplier().get();
        var result = new CompletableFuture<I>();
        if (collection.isEmpty()) {
            result.completeAsync(() -> collector.finisher().apply(a));
        } else {
            var count = new AtomicInteger(collection.size());
            var errors = new ConcurrentLinkedQueue<Throwable>();
            var accumulator = concurrentAcc(collector);

            collection.forEach(cf -> cf.whenComplete((t, th) -> {
                if (t != null) {
                    accumulator.accept(a, t);
                }
                if (th != null) {
                    errors.add(th);
                }
                if (count.decrementAndGet() == 0) {
                    if (errors.isEmpty()) {
                        result.completeAsync(() -> collector.finisher().apply(a));
                    } else {
                        result.completeExceptionally(new AccumulatedThrowable(List.copyOf(errors)));
                    }
                }
            }));
        }
        return result;
    }

    private static <T, A, R> BiConsumer<A, T> concurrentAcc(Collector<T, A, R> collector) {
        var concurrent = collector.characteristics().contains(Collector.Characteristics.CONCURRENT);
        var lock = new Object();
        return (a, t) -> {
            if (concurrent) {
                collector.accumulator().accept(a, t);
            } else {
                synchronized (lock) {
                    collector.accumulator().accept(a, t);
                }
            }
        };
    }

    @AllArgsConstructor
    @Getter
    public static class AccumulatedThrowable extends RuntimeException {
        private final List<Throwable> accumulatedExceptions;

        @Override
        public String getMessage() {
            return "Accumulated messages: \n  " + accumulatedExceptions.stream().map(Throwable::getMessage).collect(Collectors.joining("\n  "));
        }
    }
}