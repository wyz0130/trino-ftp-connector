package org.ebyhr.trino.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorInsertTableHandle;

import java.util.List;

public class StorageInsertTableHandle implements ConnectorInsertTableHandle
{

    private final StorageTableHandle storageTableHandle;

    private final StorageTable storageTable;


    private final List<StorageColumnHandle> columnHandles;


    @JsonCreator
    public StorageInsertTableHandle(@JsonProperty("storageTableHandle") StorageTableHandle storageTableHandle,
                                    @JsonProperty("storageTable") StorageTable storageTable, @JsonProperty(
                                            "storageColumnHandles") List<StorageColumnHandle> columnHandles)
    {
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

    @Override
    public String toString()
    {
        return "StorageInsertTableHandle{" + "storageTableHandle=" + storageTableHandle + ", storageTable=" + storageTable + ", columnHandles=" + columnHandles + '}';
    }
}
