package org.ebyhr.trino.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import org.ebyhr.trino.storage.dto.FtpConfig;

import java.util.List;

public class StorageInsertTableHandle implements ConnectorInsertTableHandle
{

    private final StorageTableHandle storageTableHandle;

    private final StorageTable storageTable;


    private final List<StorageColumnHandle> columnHandles;


    private final FtpConfig ftpConfig;


    @JsonCreator
    public StorageInsertTableHandle(@JsonProperty("storageTableHandle") StorageTableHandle storageTableHandle,
                                    @JsonProperty("storageTable") StorageTable storageTable,
                                    @JsonProperty("storageColumnHandles") List<StorageColumnHandle> columnHandles,
                                    @JsonProperty("ftpConfig") FtpConfig ftpConfig

    )
    {
        this.ftpConfig = ftpConfig;
        this.storageTableHandle = storageTableHandle;
        this.storageTable = storageTable;
        this.columnHandles = columnHandles;
    }


    @JsonProperty
    public StorageTableHandle getStorageTableHandle()
    {
        return storageTableHandle;
    }

    @JsonProperty
    public StorageTable getStorageTable()
    {
        return storageTable;
    }

    @JsonProperty
    public List<StorageColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }
    @JsonProperty
    public FtpConfig getFtpConfig()
    {
        return ftpConfig;
    }

    @Override
    public String toString()
    {
        return "StorageInsertTableHandle{" + "storageTableHandle=" + storageTableHandle + ", storageTable=" + storageTable + ", columnHandles=" + columnHandles + ", ftpConfig=" + ftpConfig + '}';
    }
}
