package ru.mail.polis.shakhmin;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jetbrains.annotations.NotNull;

import com.google.common.collect.Iterators;

import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import static ru.mail.polis.shakhmin.Value.TOMBSTONE_DATA;

public final class LSMDao implements DAO {

    @NotNull
    private final MemTable memTable;

    @NotNull
    private final List<Table> ssTables = new ArrayList<>();
    private final long flushThresholdInBytes;

    /**
     * Constructs a new DAO based on LSM tree.
     *
     * @param flushDir local disk folder to persist the data to
     * @param flushThresholdInBytes threshold of Memtable's size
     * @throws IOException if an I/O error is thrown by a visitor method
     */
    public LSMDao(
            @NotNull final File flushDir,
            final long flushThresholdInBytes) throws IOException {
        this.flushThresholdInBytes = flushThresholdInBytes;
        this.memTable = new MemTable(flushDir);
        Files.walkFileTree(flushDir.toPath(), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(
                    final Path file,
                    final BasicFileAttributes attrs) throws IOException {
                ssTables.add(new SSTable(file.toFile()));
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
        final var merged = Iterators.mergeSorted(iterators, Row.COMPARATOR);
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
            memTable.flush(nameFlushedTable());
        }
        memTable.upsert(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        if (memTable.sizeInBytes()
                + Row.getSizeOfFlushedRow(key, TOMBSTONE_DATA) >= flushThresholdInBytes) {
            memTable.flush(nameFlushedTable());
        }
        memTable.remove(key);
    }

    @Override
    public void close() throws IOException {
        memTable.flush(nameFlushedTable());
    }

    @NotNull
    private String nameFlushedTable() {
        return "SSTable_" + LocalDateTime.now();
    }
}
