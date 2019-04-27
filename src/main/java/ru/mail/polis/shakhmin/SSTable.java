package ru.mail.polis.shakhmin;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;

import org.jetbrains.annotations.NotNull;


public final class SSTable implements Table {

    private final long rowsNumber;

    @NotNull
    private final LongBuffer offsets;

    @NotNull
    private final ByteBuffer rows;

    public SSTable(@NotNull final File file) throws IOException {
        try (final var fc = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            final var mapped = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size())
                    .order(ByteOrder.BIG_ENDIAN);
            this.rowsNumber = mapped.getLong(mapped.limit() - Long.BYTES);

            final var offsetsBuffer = mapped.duplicate()
                    .position((int) (mapped.limit() - Long.BYTES - Long.BYTES * rowsNumber))
                    .limit(mapped.limit() - Long.BYTES);
            this.offsets = offsetsBuffer.slice().asLongBuffer();

            this.rows = mapped.duplicate()
                    .limit(offsetsBuffer.position())
                    .slice().asReadOnlyBuffer();
        }
    }

    @NotNull
    private ByteBuffer rowAt(final long offsetPosition) {
        final long offset = offsets.get((int) offsetPosition);
        final long rowSize = (offsetPosition == rowsNumber - 1)
                ? rows.limit() - offset
                : offsets.get((int) (offsetPosition + 1)) - offset;
        return rows.duplicate()
                .position((int) offset)
                .limit((int) (rowSize + offset))
                .slice().asReadOnlyBuffer();
    }

    @NotNull
    private ByteBuffer keyAt(@NotNull final ByteBuffer row) {
        final var rowBuffer = row.duplicate();
        final int keySize = rowBuffer.getInt();
        return rowBuffer.limit(keySize + Integer.BYTES)
                .slice()
                .asReadOnlyBuffer();
    }

    private long timestampAt(@NotNull final ByteBuffer row) {
        final var rowBuffer = row.duplicate();
        final int keySize = rowBuffer.getInt();
        return rowBuffer.position(keySize + Integer.BYTES)
                .getLong();
    }

    @NotNull
    private ByteBuffer valueAt(@NotNull final ByteBuffer row) {
        final var rowBuffer = row.duplicate();
        final int keySize = rowBuffer.getInt();
        return rowBuffer.position(keySize + Integer.BYTES + Long.BYTES * 2)
                .slice()
                .asReadOnlyBuffer();
    }

    @NotNull
    private Row transform(final long offsetPosition) {
        final var row = rowAt(offsetPosition);
        final var key = keyAt(row);
        final var timestamp = timestampAt(row);
        var value = (timestamp < 0)
                ? Value.tombstone(-timestamp)
                : Value.of(timestamp, valueAt(row));
        return Row.of(key, value);
    }

    private long position(@NotNull final ByteBuffer key) {
        long left = 0;
        long right = rowsNumber - 1;
        while(left <= right) {
            final long mid = (left + right) >>> 1;
            final int cmp = keyAt(rowAt(mid)).compareTo(key);
            if (cmp < 0)
                left = mid + 1;
            else if (cmp > 0)
                right = mid - 1;
            else
                return mid;
        }
        return left;
    }

    @NotNull
    @Override
    public Iterator<Row> iterator(@NotNull final ByteBuffer from) throws IOException {
        long position = position(from);
        final var rows = new ArrayList<Row>();
        for (; position < rowsNumber; position++) {
            rows.add(transform(position));
        }
        return rows.iterator();
    }

    @Override
    public void upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long sizeInBytes() {
        throw new UnsupportedOperationException();
    }
}
