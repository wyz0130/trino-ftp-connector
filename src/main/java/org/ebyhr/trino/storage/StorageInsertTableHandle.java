package org.ebyhr.trino.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorInsertTableHandle;

public class StorageInsertTableHandle implements ConnectorInsertTableHandle
{

    private final StorageTableHandle storageTableHandle;

    private final StorageTable storageTable;


    @JsonCreator
    public StorageInsertTableHandle(@JsonProperty("storageTableHandle") StorageTableHandle storageTableHandle,
                                    @JsonProperty("storageTable") StorageTable storageTable)
    {
        this.storageTableHandle = storageTableHandle;
        this.storageTable = storageTable;
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

    @Override
    public String toString()
    {
        return "StorageInsertTableHandle{" + "storageTableHandle='" + storageTableHandle + '\'' + ", storageTable=" + storageTable + '}';
    }
}
