package ru.mail.polis.shakhmin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.jetbrains.annotations.NotNull;

/**
 * A sorted collection for storing rows ({@link Row}).
 *<p>
 * Each instance of this interface must have a serial number,
 * which indicates the relevance of the storing data.
 * </p>
 */
public interface Table {

    ByteBuffer LOWEST_KEY = ByteBuffer.allocate(0);

    @NotNull
    Iterator<Row> iterator(@NotNull ByteBuffer from) throws IOException;

    void upsert(
            @NotNull ByteBuffer key,
            @NotNull ByteBuffer value) throws IOException;

    void remove(@NotNull ByteBuffer key) throws IOException;

    long sizeInBytes();

    long serialNumber();
}
