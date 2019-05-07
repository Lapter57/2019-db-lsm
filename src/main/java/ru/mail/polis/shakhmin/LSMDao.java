package ru.mail.polis.shakhmin;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.jetbrains.annotations.NotNull;

import com.google.common.collect.Iterators;

import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import static ru.mail.polis.shakhmin.Value.TOMBSTONE_DATA;

public final class LSMDao implements DAO {

    private static final String SUFFIX = ".bin";
    private static final String PREFIX = "SSTable_";
    private static final String REGEX = PREFIX + "\\d+" + SUFFIX;

    @NotNull private final MemTable memTable = new MemTable();
    @NotNull private final List<Table> ssTables = new ArrayList<>();
    @NotNull private final File flushDir;
    @NotNull private final AtomicLong serialNumberSStable;
    private final long flushThresholdInBytes;

    /**
     * Constructs a new DAO based on LSM tree.
     *
     * @param flushDir local disk folder to persist the data to
     * @param flushThresholdInBytes threshold of size of Memtable
     * @throws IOException if an I/O error is thrown by a visitor method
     */
    public LSMDao(
            @NotNull final File flushDir,
            final long flushThresholdInBytes) throws IOException {
        this.flushThresholdInBytes = flushThresholdInBytes;
        this.flushDir = flushDir;
        this.serialNumberSStable = new AtomicLong();
        Files.walkFileTree(flushDir.toPath(), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(
                    final Path path,
                    final BasicFileAttributes attrs) throws IOException {
                final File file = path.toFile();
                if (file.getName().matches(REGEX)) {
                    final String fileName = file.getName().split("\\.")[0];
                    final long serialNumber = Long.valueOf(fileName.split("_")[1]);
                    serialNumberSStable.set(
                            Math.max(serialNumberSStable.get(), serialNumber + 1L));
                    ssTables.add(new SSTable(file.toPath(), serialNumber));
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final var memIterator = memTable.iterator(from);
        final var iterators = new ArrayList<Iterator<Row>>();
        iterators.add(memIterator);
        for (final var ssTable: ssTables) {
            iterators.add(ssTable.iterator(from));
        }
        final var merged = Iterators.mergeSorted(iterators, Row::compareTo);
        final var collapsed = Iters.collapseEquals(merged, Row::getKey);
        final var alive = Iterators.filter(collapsed, r -> !r.getValue().isRemoved());
        return Iterators.transform(alive,
                r -> Record.of(r.getKey(), r.getValue().getData()));
    }

    @Override
    public void upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) throws IOException {
        if (memTable.sizeInBytes()
                + Row.getSizeOfFlushedRow(key, value) >= flushThresholdInBytes) {
            flushAndLoad();
        }
        memTable.upsert(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        if (memTable.sizeInBytes()
                + Row.getSizeOfFlushedRow(key, TOMBSTONE_DATA) >= flushThresholdInBytes) {
            flushAndLoad();
        }
        memTable.remove(key);
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    private void flush() throws IOException {
        final var fileName = nameFlushedTable();
        SSTable.flush(
                Path.of(flushDir.getAbsolutePath(), fileName + SUFFIX),
                memTable.iterator(ByteBuffer.allocate(0)));
        memTable.clear();
    }

    private void flushAndLoad() throws IOException {
        final var path = Path.of(flushDir.getAbsolutePath(),
                nameFlushedTable() + SUFFIX);
        SSTable.flush(
                path,
                memTable.iterator(ByteBuffer.allocate(0)));
        ssTables.add(new SSTable(
                path.toAbsolutePath(),
                serialNumberSStable.get() - 1L));
        memTable.clear();
    }

    @NotNull
    private String nameFlushedTable() {
        return PREFIX + serialNumberSStable.getAndIncrement();
    }
}
