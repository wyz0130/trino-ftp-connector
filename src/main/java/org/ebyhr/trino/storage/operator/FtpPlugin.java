package org.ebyhr.trino.storage.operator;


import io.airlift.log.Logger;
import io.trino.spi.Page;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.ebyhr.trino.storage.StorageColumnHandle;
import org.ebyhr.trino.storage.StorageConfig;
import org.ebyhr.trino.storage.utils.FtpUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.trino.spi.type.VarcharType.VARCHAR;

public class FtpPlugin implements FilePlugin
{
    private static final Logger log = Logger.get(FtpPlugin.class);

    @Override
    public List<StorageColumnHandle> getFields(String path, Function<String, InputStream> streamProvider, StorageConfig storageConfig)
    {
//        FTPClient ftpClient = FtpUtils.getFTPClient(storageConfig.getFtpHost(), storageConfig.getFtpPort(), storageConfig.getFtpUser(), storageConfig.getFtpPassWord());
        return List.of(new StorageColumnHandle("value", VARCHAR));
    }

    @Override
    public Stream<List<?>> getRecordsIterator(String path, Function<String, InputStream> streamProvider)
    {
        log.info("path :"+path);
        return  Stream.of(List.of(path));
    }
}
