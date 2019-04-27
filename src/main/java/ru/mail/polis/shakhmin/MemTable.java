package ru.mail.polis.shakhmin;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.jetbrains.annotations.NotNull;

public final class MemTable implements Table {

    public static final String SUFFIX = ".txt";

    @NotNull
    private NavigableMap<ByteBuffer, Row> storage = new TreeMap<>();
    private long sizeInBytes;

    @NotNull
    private final File flushDir;

    public MemTable(@NotNull final File flushDir) {
        this.flushDir = flushDir;
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
        final var prev = storage.get(key);
        if (prev == null) {
            sizeInBytes += Row.getSizeOfFlushedRow(key, value);
        } else {
            sizeInBytes += value.remaining();
        }
        storage.put(key, Row.of(key, Value.of(System.currentTimeMillis(), value)));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        final var tombstone = Value.tombstone(System.currentTimeMillis());
        final var prev = storage.put(key, Row.of(key, tombstone));
        if (prev == null) {
            sizeInBytes += Row.getSizeOfFlushedRow(key, tombstone.getData());
        } else if (!prev.getValue().isRemoved()){
            sizeInBytes -= prev.getValue().getData().remaining();
        }
    }

    /**
     * Flush of data to disk as SSTable.
     *
     * @param fileName the name of the file to which the data is flushed
     * @throws IOException if an I/O error occurs
     */
    public void flush(@NotNull final String fileName) throws IOException {
        long offset = 0L;
        final var offsets = new ArrayList<Long>();
        offsets.add(offset);
        try (var fc = FileChannel.open(
                Path.of(flushDir.getAbsolutePath(), fileName + SUFFIX),
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            for (final var row: storage.values()) {
                final var key = row.getKey();
                final var value = row.getValue();
                final var sizeRow = Row.getSizeOfFlushedRow(key, value.getData());
                final var rowBuffer = ByteBuffer.allocate((int) sizeRow)
                        .putInt(key.remaining())
                        .put(key.duplicate())
                        .putLong(value.getTimestamp());
                if (!value.isRemoved()) {
                    final var data = value.getData();
                    rowBuffer.putLong(data.remaining())
                             .put(data.duplicate());
                }
                offset += sizeRow;
                offsets.add(offset);
                rowBuffer.flip();
                fc.write(rowBuffer);
            }
            offsets.remove(offsets.size() - 1);
            final var offsetsBuffer = ByteBuffer.allocate(
                    offsets.size() * Long.BYTES + Long.BYTES);
            for (final var anOffset: offsets) {
                offsetsBuffer.putLong(anOffset);
            }
            offsetsBuffer.putLong(offsets.size())
                         .flip();
            fc.write(offsetsBuffer);
        }
        storage.clear();
        sizeInBytes = 0;
    }

    @Override
    public long sizeInBytes() {
        return sizeInBytes;
    }
}
