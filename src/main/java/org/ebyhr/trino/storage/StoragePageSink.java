package org.ebyhr.trino.storage;


import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.commons.net.ftp.FTPClient;
import org.ebyhr.trino.storage.utils.FtpUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class StoragePageSink implements ConnectorPageSink
{

    private static final Logger log = Logger.get(StoragePageSink.class);


    private final StorageClient storageClient;
    private final StorageTableHandle storageTableHandle;

    private final StorageTable storageTable;

    private final List<Type> types;
    private final List<String> columns;

    private final StorageConfig storageConfig;

    private static String FILE = "file://";

    boolean flag = true;

    private String fileName;

    private StringBuilder tableData;


    public StoragePageSink(StorageClient storageClient, StorageTableHandle storageTableHandle,
                           StorageTable storageTable, List<Type> types, List<String> columns)
    {
        this.storageClient = storageClient;
        this.storageConfig = storageClient.getStorageConfig();
        this.storageTableHandle = storageTableHandle;
        this.storageTable = storageTable;
        this.types = types;
        this.columns = columns;

    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        log.info("page.getPositionCount() :" + page.getPositionCount());
        StringBuilder stringBuilder = new StringBuilder();
        if (flag) {
            for (String column : columns) {
                stringBuilder.append(column);
                stringBuilder.append(",");
            }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            stringBuilder.append("\n");
            for (int position = 0; position < page.getPositionCount(); position++) {
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    String s = appendColumn(page, position, channel);
                    stringBuilder.append(s);
                    if (channel != page.getChannelCount() - 1) {
                        stringBuilder.append(",");
                    }
                }
                if (position != page.getPositionCount() - 1) {
                    stringBuilder.append("\n");
                }
            }
            log.info("stringBuilder " + stringBuilder.length());
            FtpWrite(stringBuilder);
            flag = false;
        }
        else {
            try {
                stringBuilder.append("\n");
                for (int position = 0; position < page.getPositionCount(); position++) {
                    for (int channel = 0; channel < page.getChannelCount(); channel++) {
                        String s = appendColumn(page, position, channel);
                        stringBuilder.append(s);
                        if (channel != page.getChannelCount() - 1) {
                            stringBuilder.append(",");
                        }
                    }
                    if (position != page.getPositionCount() - 1) {
                        stringBuilder.append("\n");
                    }
                }
                FTPClient ftpClient = FtpUtils.getFTPClient(storageClient.getStorageConfig());
                OutputStream outputStream = ftpClient.appendFileStream(storageConfig.getPath() + "/" + fileName);
                outputStream.write(stringBuilder.toString().getBytes());
                outputStream.flush();
                outputStream.close();
                ftpClient.logout();
            }
            catch (Exception e) {
                log.error("appendPage :" + e.getMessage());
            }
        }
        return NOT_BLOCKED;
    }

    private String appendColumn(Page page, int position, int channel)
    {
        Block block = page.getBlock(channel);

        Type type = types.get(channel);
        if (INTEGER.equals(type)) {
            long aLong = type.getLong(block, position);
            return String.valueOf(aLong);
        }
        else if (BOOLEAN.equals(type)) {
            boolean aBoolean = type.getBoolean(block, position);
            return String.valueOf(aBoolean);
        }
        else if (DOUBLE.equals(type)) {
            double aDouble = type.getDouble(block, position);
            return String.valueOf(aDouble);
        }
        else if (type.getClass().getSuperclass().equals(DecimalType.class)) {
            BigDecimal value = ((SqlDecimal) type.getObjectValue(null, block, position)).toBigDecimal();
            return String.valueOf(value);
        }
        else if (type.equals(VarcharType.VARCHAR)) {
            Slice slice = type.getSlice(block, position);
            return slice.toStringUtf8();
        }
        else if (TimestampType.TIMESTAMP_MILLIS.equals(type)) {
            long aLong = type.getLong(block, position);
            return new SqlDate(Integer.parseInt(String.valueOf(aLong))).toString();
        }
        return null;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        // the committer does not need any additional info.
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {

    }


    public FTPClient FtpWrite(StringBuilder stringBuilder)
    {
        FTPClient ftpClient = FtpUtils.getFTPClient(storageClient.getStorageConfig());
        InputStream is = null;

        try {
            int reply;
            //1.输入流
            is = new ByteArrayInputStream(stringBuilder.toString().getBytes());
            //2.指定写入的目录
            ftpClient.changeWorkingDirectory(storageConfig.getPath());
            //3.写操作
            ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
            fileName =
                    new StringBuilder(storageConfig.getTable() + "_" + new Date().getTime() + "." + storageConfig.getSchema()).toString();
            fileName = new String(fileName.getBytes("utf-8"), "iso-8859-1");
            ftpClient.storeFile(fileName, is);
            log.info("FtpWrite :" + fileName);
            is.close();
            //退出
            return ftpClient;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            try {
                if (is != null) {
                    is.close();
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
