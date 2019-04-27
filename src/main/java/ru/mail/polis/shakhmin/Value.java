package ru.mail.polis.shakhmin;

import java.nio.ByteBuffer;

import org.jetbrains.annotations.NotNull;

public final class Value implements Comparable<Value> {

    private final long timestamp;

    @NotNull
    private final ByteBuffer data;

    private Value(
            final long timestamp,
            @NotNull final ByteBuffer data) {
        this.timestamp = timestamp;
        this.data = data;
    }

    @NotNull
    public static Value of(
            final long timestamp,
            @NotNull final ByteBuffer data) {
        return new Value(timestamp, data);
    }

    @NotNull
    public static Value tombstone(final long timestamp) {
        return new Value(-timestamp, ByteBuffer.allocate(0));
    }

    @NotNull
    public ByteBuffer getData() {
        return data.asReadOnlyBuffer();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isRemoved() {
        return timestamp < 0;
    }

    @Override
    public int compareTo(@NotNull final Value value) {
        return Long.compare((value.timestamp < 0)? -value.timestamp : value.timestamp,
                (timestamp < 0)? -timestamp: timestamp);
    }
}
