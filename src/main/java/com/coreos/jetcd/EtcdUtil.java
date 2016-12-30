package com.coreos.jetcd;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

import com.coreos.jetcd.api.Event;
import com.coreos.jetcd.api.LeaseGrantResponse;
import com.coreos.jetcd.api.ResponseHeader;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.EtcdHeader;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.lease.Lease;
import com.coreos.jetcd.watch.WatchEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * This util is to convert api class to client class.
 */
class EtcdUtil {

    private EtcdUtil() {
    }

    /**
     * convert ByteSequence to ByteString
     */
    protected static ByteString byteStringFromByteSequence(ByteSequence byteSequence) {
        return ByteString.copyFrom(byteSequence.getBytes());
    }

    /**
     * convert ByteString to ByteSequence
     */
    protected static ByteSequence byteSequceFromByteString(ByteString byteString) {
        return ByteSequence.fromBytes(byteString.toByteArray());
    }

    /**
     * convert API KeyValue to etcd client KeyValue
     */
    protected static KeyValue apiToClientKV(com.coreos.jetcd.api.KeyValue keyValue) {
        if (keyValue == null) {
            return null;
        }
        return new KeyValue(
                byteSequceFromByteString(keyValue.getKey()),
                byteSequceFromByteString(keyValue.getValue()),
                keyValue.getCreateRevision(),
                keyValue.getModRevision(),
                keyValue.getVersion(),
                keyValue.getLease());
    }

    /**
     * convert API watch event to etcd client event
     */
    protected static WatchEvent apiToClientEvent(Event event) {
        WatchEvent.EventType eventType = WatchEvent.EventType.UNRECOGNIZED;
        switch (event.getType()) {
            case DELETE:
                eventType = WatchEvent.EventType.DELETE;
                break;
            case PUT:
                eventType = WatchEvent.EventType.PUT;
                break;
        }
        return new WatchEvent(apiToClientKV(event.getKv()), apiToClientKV(event.getPrevKv()), eventType);
    }

    protected static List<WatchEvent> apiToClientEvents(List<Event> events) {
        List<WatchEvent> watchEvents = new ArrayList<>();
        for (Event event : events) {
            watchEvents.add(apiToClientEvent(event));
        }
        return watchEvents;
    }

    /**
     * convert API response header to self defined header
     */
    protected static EtcdHeader apiToClientHeader(ResponseHeader header) {
        if (header == null) {
            return null;
        }
        return new EtcdHeader(header.getClusterId(), header.getMemberId(), header.getRevision(), header.getRaftTerm());
    }

    protected static Lease apiToClientLease(LeaseGrantResponse response) {
        return new Lease(response.getID(), response.getTTL(), apiToClientHeader(response.getHeader()));
    }

    static <S, T> CompletableFuture<T> completableFromListenableFuture(final ListenableFuture<S> sourceFuture, final Converter<S, T> resultConvert, Executor executor) {
        CompletableFuture<T> targetFuture = new CompletableFuture<T>() {
            /**
             * If not already completed, completes this CompletableFuture with
             * a {@link CancellationException}. Dependent CompletableFutures
             * that have not already completed will also complete
             * exceptionally, with a {@link CompletionException} caused by
             * this {@code CancellationException}.
             *
             * @param mayInterruptIfRunning this value has no effect in this
             *                              implementation because interrupts are not used to control
             *                              processing.
             * @return {@code true} if this task is now cancelled
             */
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                boolean result = sourceFuture.cancel(mayInterruptIfRunning);
                super.cancel(mayInterruptIfRunning);
                return result;
            }
        };
        sourceFuture.addListener(() -> {
            try {
                targetFuture.complete(resultConvert.convert(sourceFuture.get()));
            } catch (Exception e) {
                targetFuture.completeExceptionally(e);
            }
        }, executor);
        return targetFuture;
    }


    interface Converter<S, T> {
        T convert(S source);
    }
}
