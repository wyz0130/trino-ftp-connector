package org.ebyhr.trino.storage;


import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;

public class StoragePageSinkProvider implements ConnectorPageSinkProvider
{
    private static final Logger log = Logger.get(StoragePageSinkProvider.class);

    private final StorageClient storageClient;

    @Inject
    public StoragePageSinkProvider(StorageClient storageClient)
    {
        log.info("StoragePageSinkProvider :");
        this.storageClient = storageClient;

    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                            ConnectorOutputTableHandle outputTableHandle,
                                            ConnectorPageSinkId pageSinkId)
    {

        return new StoragePageSink(storageClient, null, null, null, null);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                            ConnectorInsertTableHandle insertTableHandle,
                                            ConnectorPageSinkId pageSinkId)
    {
        StorageInsertTableHandle storageInsertTableHandle = (StorageInsertTableHandle) insertTableHandle;


        List<Type> types = new ArrayList<>();
        List<String> columns = new ArrayList<>();
        List<StorageColumnHandle> columnHandles = storageInsertTableHandle.getStorageTable().getColumns();
        log.info("storageInsertTableHandle :" + columnHandles.toString());

        for (StorageColumnHandle column : columnHandles) {
            types.add(column.getType());
            columns.add(column.getName());
        }
        return new StoragePageSink(storageClient, storageInsertTableHandle.getStorageTableHandle(),
                storageInsertTableHandle.getStorageTable(), types, columns);
    }

}
