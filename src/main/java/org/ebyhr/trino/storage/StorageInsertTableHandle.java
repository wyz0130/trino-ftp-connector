package org.ebyhr.trino.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.type.Type;
import org.ebyhr.trino.storage.dto.FtpConfig;

import java.util.List;

public class StorageInsertTableHandle implements ConnectorInsertTableHandle
{

    private final StorageTableHandle storageTableHandle;

    private final StorageTable storageTable;


    private final List<StorageColumnHandle> columnHandles;


    private final FtpConfig ftpConfig;

    private final List<Type> columntypes;
    private final List<String> columnNames;


    @JsonCreator
    public StorageInsertTableHandle(@JsonProperty("storageTableHandle") StorageTableHandle storageTableHandle,
                                    @JsonProperty("storageTable") StorageTable storageTable, @JsonProperty(
                                            "storageColumnHandles") List<StorageColumnHandle> columnHandles,
                                    @JsonProperty("ftpConfig") FtpConfig ftpConfig,
                                    @JsonProperty("columntypes") List<Type> columntypes,
                                    @JsonProperty("columnNames") List<String> columnNames

    )
    {
        this.ftpConfig = ftpConfig;
        this.storageTableHandle = storageTableHandle;
        this.storageTable = storageTable;
        this.columnHandles = columnHandles;
        this.columntypes = columntypes;
        this.columnNames = columnNames;
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

    @JsonProperty
    public List<Type> getColumntypes()
    {
        return columntypes;
    }

    @JsonProperty
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @Override
    public String toString()
    {
        return "StorageInsertTableHandle{" + "storageTableHandle=" + storageTableHandle + ", storageTable=" + storageTable + ", columnHandles=" + columnHandles + ", ftpConfig=" + ftpConfig + ", columntypes=" + columntypes + ", columnNames=" + columnNames + '}';
    }
}
