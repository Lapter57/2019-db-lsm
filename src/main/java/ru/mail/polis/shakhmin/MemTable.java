package ru.mail.polis.shakhmin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.jetbrains.annotations.NotNull;

public final class MemTable implements Table {

    @NotNull private final NavigableMap<ByteBuffer, Row> storage = new TreeMap<>();
    private long sizeInBytes;
    private final long serialNumber = Long.MAX_VALUE;

    public MemTable() {
    }

    @NotNull
    @Override
    public Iterator<Row> iterator(@NotNull final ByteBuffer from) throws IOException {
        return storage.tailMap(from).values().iterator();
    }

    @Override
    public void upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) throws IOException {
        final var prev = storage.put(key, Row.of(
                key,
                Value.of(System.currentTimeMillis(), value),
                serialNumber));
        if (prev == null) {
            sizeInBytes += Row.getSizeOfFlushedRow(key, value);
        } else {
            sizeInBytes += value.remaining();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        final var tombstone = Value.tombstone(System.currentTimeMillis());
        final var prev = storage.put(key, Row.of(key, tombstone, serialNumber));
        if (prev == null) {
            sizeInBytes += Row.getSizeOfFlushedRow(key, tombstone.getData());
        } else if (!prev.getValue().isRemoved()){
            sizeInBytes -= prev.getValue().getData().remaining();
        }
    }

    public Collection<Row> getRows() {
        return storage.values();
    }

    public void clear() {
        storage.clear();
        sizeInBytes = 0;
    }

    @Override
    public long serialNumber() {
        return serialNumber;
    }

    @Override
    public long sizeInBytes() {
        return sizeInBytes;
    }
}
