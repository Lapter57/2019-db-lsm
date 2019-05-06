package ru.mail.polis.shakhmin;

import java.nio.ByteBuffer;
import java.util.Comparator;

import org.jetbrains.annotations.NotNull;

public final class Row implements Comparable<Row> {

    @NotNull private final ByteBuffer key;
    @NotNull private final Value value;
    private final long tableId;

    private Row(
            @NotNull final ByteBuffer key,
            @NotNull final Value value,
            final long tableId) {
        this.key = key;
        this.value = value;
        this.tableId = tableId;
    }

    public static Row of(
            @NotNull final ByteBuffer key,
            @NotNull final Value value,
            final long tableId) {
        return new Row(key, value, tableId);
    }

    @NotNull
    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    @NotNull
    public Value getValue() {
        return value;
    }

    public long getTableId() {
        return tableId;
    }

    public static long getSizeOfFlushedRow(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) {
        return Integer.BYTES + key.remaining() + Long.BYTES
                + (value.remaining() == 0 ? 0 : Long.BYTES + value.remaining());
    }

    @Override
    public int compareTo(@NotNull final Row row) {
        final int cmp = Comparator.comparing(Row::getKey)
                .thenComparing(Row::getValue)
                .compare(this, row);
        if (cmp == 0) {
            return Comparator
                    .comparing(Row::getTableId)
                    .compare(row, this);
        } else {
            return cmp;
        }
    }
}
