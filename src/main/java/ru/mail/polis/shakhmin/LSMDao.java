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


import org.jetbrains.annotations.NotNull;

import com.google.common.collect.Iterators;

import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

public final class LSMDao implements DAO {

    @NotNull
    private MemTable memTable;

    @NotNull
    private final List<Table> ssTables = new ArrayList<>();
    private final long flushThreshold;
    private long numFlushedTables;

    public LSMDao(
            @NotNull final File file,
            final long flushThreshold) throws IOException {
        this.flushThreshold = flushThreshold;
        this.memTable = new MemTable(file);
        Files.walkFileTree(file.toPath(), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                ssTables.add(new SSTable(file.toFile()));
                numFlushedTables++;
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final var memIterator = memTable.iterator(from);
        Iterator<Row> alive;
        if (!ssTables.isEmpty()) {
            final var iterators = new ArrayList<Iterator<Row>>();
            iterators.add(memIterator);
            for (final var ssTable: ssTables) {
                iterators.add(ssTable.iterator(from));
            }
            final var merged = Iterators.mergeSorted(iterators, Row.COMPARATOR);
            final var collapsed = Iters.collapseEquals(merged);
            alive = aliveIterator(collapsed);
        } else {
            alive = aliveIterator(memIterator);
        }
        return Iterators.transform(alive,
                r -> Record.of(r.getKey(), r.getValue().getData()));
    }

    @NotNull
    private Iterator<Row> aliveIterator(@NotNull final Iterator<Row> iter) {
        final var dead = new ArrayList<>();
        return Iterators.filter(iter, r -> {
            if(!r.getValue().isRemoved() && !dead.contains(r.getKey())) {
                return true;
            } else if (r.getValue().isRemoved()){
                dead.add(r.getKey());
            }
            return false;
        });
    }

    @Override
    public void upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) throws IOException {
        if (memTable.sizeInBytes() + Row.getSizeOfFlushedRow(key, value) >= flushThreshold) {
            memTable.flush(nameFlushedTable());
        }
        memTable.upsert(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
    }

    @Override
    public void close() throws IOException {
        memTable.flush(nameFlushedTable());
    }

    @NotNull
    private String nameFlushedTable() {
        return "SSTable_" + ++numFlushedTables;
    }
}
