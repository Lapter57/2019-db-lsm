package ru.mail.polis.shakhmin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.jetbrains.annotations.NotNull;


public interface Table {
    @NotNull
    Iterator<Row> iterator(@NotNull ByteBuffer from) throws IOException;

    void upsert(
            @NotNull ByteBuffer key,
            @NotNull ByteBuffer value) throws IOException;

    void remove(@NotNull ByteBuffer key) throws IOException;

    long sizeInBytes();
}