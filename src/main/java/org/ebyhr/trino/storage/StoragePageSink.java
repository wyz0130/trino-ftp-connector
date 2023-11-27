package org.ebyhr.trino.storage;


import com.alibaba.fastjson.JSON;
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
import org.apache.commons.net.ftp.FTPFile;
import org.ebyhr.trino.storage.dto.FtpConfig;
import org.ebyhr.trino.storage.utils.Constant;
import org.ebyhr.trino.storage.utils.FtpUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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

    private final FtpConfig ftpConfig;

    private static String FILE = "file://";

    boolean fileIsExist = false;

    private String fileName;


    public StoragePageSink(StorageClient storageClient, StorageTableHandle storageTableHandle,
                           StorageTable storageTable, List<Type> types, List<String> columns, FtpConfig ftpConfig)
    {
        this.storageClient = storageClient;
        this.ftpConfig = ftpConfig;
        this.storageTableHandle = storageTableHandle;
        this.storageTable = storageTable;
        this.types = types;
        this.columns = columns;


    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        StringBuilder stringBuilder = new StringBuilder();
        try {
            log.debug("appendPage");
            if (ftpConfig.getFormat().equals("json")) {
                for (int position = 0; position < page.getPositionCount(); position++) {
                    HashMap<String, Object> columnMap = new HashMap<String, Object>();
                    for (int channel = 0; channel < page.getChannelCount(); channel++) {
                        columnMap.put(columns.get(channel), appendColumn(page, position, channel));
                    }
                    stringBuilder.append(JSON.toJSONString(columnMap));
                    if (position != page.getPositionCount() - 1) {
                        stringBuilder.append(ftpConfig.getColumnSeparator());
                        stringBuilder.append(Constant.LINE_BREAK);
                    }
                }
                if (!fileIsExist && fileIsExist()) {
                    ftpWrite(stringBuilder);
                }
                else {
                    ftpAppend(stringBuilder);
                }
            }
            else {
                for (int position = 0; position < page.getPositionCount(); position++) {
                    for (int channel = 0; channel < page.getChannelCount(); channel++) {
                        stringBuilder.append(appendColumn(page, position, channel));
                        if (channel != page.getChannelCount() - 1) {
                            stringBuilder.append(ftpConfig.getColumnSeparator());
                        }
                    }
                    if (position != page.getPositionCount() - 1) {
                        stringBuilder.append(Constant.LINE_BREAK);
                    }
                }
                if (!fileIsExist && fileIsExist()) {
                    StringBuilder columnNames = new StringBuilder();
                    for (String column : columns) {
                        columnNames.append(column);
                        columnNames.append(ftpConfig.getColumnSeparator());
                    }
                    columnNames.deleteCharAt(stringBuilder.length() - 1);
                    columnNames.append(Constant.LINE_BREAK);
                    stringBuilder.insert(0, columnNames);
                    ftpWrite(stringBuilder);
                }
                else {
                    ftpAppend(stringBuilder);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            log.error(e, "appendPage is Exception");
            throw new RuntimeException(e);
        }
        return NOT_BLOCKED;
    }

    private Object appendColumn(Page page, int position, int channel)
    {
        Block block = page.getBlock(channel);

        Type type = types.get(channel);
        if (INTEGER.equals(type)) {
            return type.getLong(block, position);
        }
        else if (BOOLEAN.equals(type)) {
            return type.getBoolean(block, position);
        }
        else if (DOUBLE.equals(type)) {
            return type.getDouble(block, position);
        }
        else if (type.getClass().getSuperclass().equals(DecimalType.class)) {
            BigDecimal bigDecimal = ((SqlDecimal) type.getObjectValue(null, block, position)).toBigDecimal();
            return bigDecimal;
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


    public boolean fileIsExist()
    {
        try {
            fileName =
                    ftpConfig.getTable() + Constant.UNDERLINE + ftpConfig.getNodeId() + Constant.POINT + ftpConfig.getSchema();
            log.info("fileName is :" + fileName);
            FTPClient ftpClient = FtpUtils.getFTPClient(ftpConfig);
            FTPFile[] ftpFiles = ftpClient.listFiles(ftpConfig.getPath() + Constant.SEPARATOR + fileName);
            if (ftpFiles != null && ftpFiles.length > 0) {
                List<String> fileNames = Arrays.stream(ftpFiles).map(FTPFile::getName).collect(Collectors.toList());
                ftpClient.logout();
                if (fileNames.contains(fileName)) {
                    fileIsExist = true;
                }
            }
        }
        catch (IOException e) {
            log.error(e, "fileIsExist is Exception");
            throw new RuntimeException(e);
        }
        return false;
    }

    public void ftpWrite(StringBuilder stringBuilder)
    {
        if (!fileIsExist) {
            return;
        }
        FTPClient ftpClient = FtpUtils.getFTPClient(ftpConfig);
        InputStream is = null;
        try {
            //1.输入流
            is = new ByteArrayInputStream(stringBuilder.toString().getBytes());
            //2.指定写入的目录
            ftpClient.changeWorkingDirectory(ftpConfig.getPath());
            //3.写操作
            ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
            fileName = new String(fileName.getBytes(Constant.JSON_ENCODING_UTF8), Constant.JSON_ENCODING_ISO);
            ftpClient.storeFile(fileName, is);
            //退出
            is.close();
        }
        catch (IOException e) {
            e.printStackTrace();
            log.error(e, "FtpWrite is Exception");
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

    public void ftpAppend(StringBuilder stringBuilder)
    {
        try {
            stringBuilder.append("\n");
            FTPClient ftpClient = FtpUtils.getFTPClient(ftpConfig);
            OutputStream outputStream = ftpClient.appendFileStream(ftpConfig.getPath() + "/" + fileName);
            outputStream.write(stringBuilder.toString().getBytes());
            outputStream.flush();
            outputStream.close();
            ftpClient.logout();
        }
        catch (IOException e) {
            log.error(e, "ftpAppend is exception");
            throw new RuntimeException("appendPage " + e.getMessage());
        }
    }
}
