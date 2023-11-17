package org.ebyhr.trino.storage.operator;


import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.ebyhr.trino.storage.StorageColumnHandle;
import org.ebyhr.trino.storage.StorageConfig;
import org.ebyhr.trino.storage.utils.Utils;

import java.io.InputStream;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class FtpPlugin implements FilePlugin
{
    private static final Logger log = Logger.get(FtpPlugin.class);

    @Override
    public List<StorageColumnHandle> getFields(String path, Function<String, InputStream> streamProvider,
                                               StorageConfig storageConfig)
    {


//        return List.of(new StorageColumnHandle("date", VARCHAR));
        return null;
    }

    @Override
    public Stream<List<?>> getRecordsIterator(String path, Function<String, InputStream> streamProvider)
    {
        log.info("path :" + path);
        return Stream.of(List.of(path));
    }
}
