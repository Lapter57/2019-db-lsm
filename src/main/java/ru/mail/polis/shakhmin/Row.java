package ru.mail.polis.shakhmin;

import java.nio.ByteBuffer;
import java.util.Comparator;

import org.jetbrains.annotations.NotNull;

public final class Row implements Comparable<Row> {

    @NotNull
    private final ByteBuffer key;

    @NotNull
    private Value value;

    static final Comparator<Row> COMPARATOR =
            Comparator.comparing(Row::getKey)
                      .thenComparing(Row::getValue);

    private Row(
            @NotNull final ByteBuffer key,
            @NotNull final Value value) {
        this.key = key;
        this.value = value;
    }

    public static Row of(
            @NotNull final ByteBuffer key,
            @NotNull final Value value) {
        return new Row(key, value);
    }

    @NotNull
    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    @NotNull
    public Value getValue() {
        return value;
    }

    public static long getSizeOfFlushedRow(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) {
        return Integer.BYTES + key.remaining() + Long.BYTES +
                (value.remaining() == 0 ? 0 : Long.BYTES + value.remaining());
    }

    @Override
    public int compareTo(@NotNull final Row row) {
        return COMPARATOR.compare(this, row);
    }
}
