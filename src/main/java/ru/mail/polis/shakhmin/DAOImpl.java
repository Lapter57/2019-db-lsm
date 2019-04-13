package ru.mail.polis.shakhmin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.jetbrains.annotations.NotNull;

import ru.mail.polis.DAO;
import ru.mail.polis.Record;

public class DAOImpl implements DAO {
    private final NavigableMap<ByteBuffer, Record> storage = new TreeMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        return storage.tailMap(from).values().iterator();
    }

    @Override
    public void upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) throws IOException {
        storage.put(key, Record.of(key, value));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        storage.remove(key);
    }

    @Override
    public void close() {
        // without implementation
        throw new UnsupportedOperationException();
    }
}
