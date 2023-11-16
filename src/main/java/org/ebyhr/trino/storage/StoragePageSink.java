package org.ebyhr.trino.storage;


import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.VarcharType;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class StoragePageSink implements ConnectorPageSink
{

    private static final Logger log = Logger.get(StoragePageSink.class);


    private final StorageClient storageClient;
    private final StorageTableHandle storageTableHandle;

    private final StorageTable storageTable;

    private static String FILE = "file://";


    public StoragePageSink(StorageClient storageClient, StorageTableHandle storageTableHandle,
                           StorageTable storageTable)
    {
        this.storageClient = storageClient;
        this.storageTableHandle = storageTableHandle;
        this.storageTable = storageTable;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        StringBuilder stringBuilder = new StringBuilder();
        //第一步：设置输出的文件路径
        //如果该目录下不存在该文件，则文件会被创建到指定目录下。如果该目录有同名文件，那么该文件将被覆盖。
        try {
            log.info("storageTable :"+storageTable.getColumns().toString());
            for (StorageColumnHandle column : storageTable.getColumns()) {
                stringBuilder.append(column.getName());
                stringBuilder.append(",");
            }
            stringBuilder.deleteCharAt(stringBuilder.length()-1);
            stringBuilder.append("\n");
            log.info("stringBuilder :"+stringBuilder.toString());

            String path = storageTableHandle.getTableName();
            log.info("path :" + path);
            if (storageTableHandle.getTableName().contains(FILE)) {
                path = path.replace(FILE, "");
                log.info("path replace:" + path);
            }
            File writeFile = new File(path);
            //第二步：通过BufferedReader类创建一个使用默认大小输出缓冲区的缓冲字符输出流
            BufferedWriter writeText = new BufferedWriter(new FileWriter(writeFile));
            for (int position = 0; position < page.getPositionCount(); position++) {
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    String s = dorisAppendColumn(page, position, channel);
                    stringBuilder.append(s);
                    if (channel != page.getChannelCount() - 1) {
                        stringBuilder.append(",");
                    }
                }
                if (position != page.getPositionCount() - 1) {
                    stringBuilder.append("\n");
                }
                //第三步：将文档的下一行数据赋值给lineData，并判断是否为空，若不为空则输出
                //调用write的方法将字符串写到流中
                writeText.write(stringBuilder.toString());
                //使用缓冲区的刷新方法将数据刷到目的地中
                writeText.flush();
                stringBuilder = new StringBuilder();
            }
            //关闭缓冲区，缓冲区没有调用系统底层资源，真正调用底层资源的是FileWriter对象，缓冲区仅仅是一个提高效率的作用
            //因此，此处的close()方法关闭的是被缓存的流对象
            writeText.close();
        }
        catch (FileNotFoundException e) {
            log.error("FileNotFoundException not find file " + e.getMessage());
        }
        catch (IOException e) {
            log.error("IOException  file  read error" + e.getMessage());
        }
        return NOT_BLOCKED;
    }

    private String dorisAppendColumn(Page page, int position, int channel)
    {
        Block block = page.getBlock(channel);

        VarcharType varchar = VarcharType.VARCHAR;
        Slice slice = varchar.getSlice(block, position);
        String stringUtf8 = slice.toStringUtf8();
        return stringUtf8;
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
}
