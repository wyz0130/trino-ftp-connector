package org.ebyhr.trino.storage.operator;


import io.trino.spi.Page;
import org.ebyhr.trino.storage.StorageColumnHandle;

import java.io.InputStream;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class FtpPlugin implements FilePlugin
{
    @Override
    public List<StorageColumnHandle> getFields(String path, Function<String, InputStream> streamProvider)
    {
        return null;
    }

    @Override
    public Stream<List<?>> getRecordsIterator(String path, Function<String, InputStream> streamProvider)
    {
        return FilePlugin.super.getRecordsIterator(path, streamProvider);
    }

    @Override
    public Iterable<Page> getPagesIterator(String path, Function<String, InputStream> streamProvider)
    {
        return FilePlugin.super.getPagesIterator(path, streamProvider);
    }
}
