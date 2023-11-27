package org.ebyhr.trino.storage;


import com.google.inject.Inject;

import io.airlift.log.Logger;
import io.airlift.node.NodeConfig;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import org.ebyhr.trino.storage.dto.FtpConfig;
import org.ebyhr.trino.storage.utils.Constant;

public class StoragePageSinkProvider implements ConnectorPageSinkProvider
{
    private static final Logger log = Logger.get(StoragePageSink.class);

    private final StorageClient storageClient;

    private NodeConfig nodeConfig;

    @Inject
    public StoragePageSinkProvider(StorageClient storageClient, NodeConfig nodeConfig)
    {
        this.storageClient = storageClient;
        this.nodeConfig = nodeConfig;

    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                            ConnectorOutputTableHandle outputTableHandle,
                                            ConnectorPageSinkId pageSinkId)
    {
        log.debug("2222222222");
        return new StoragePageSink(storageClient, null, null, null, null, null);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                            ConnectorInsertTableHandle insertTableHandle,
                                            ConnectorPageSinkId pageSinkId)
    {
        log.debug("11111111111");
        StorageInsertTableHandle storageInsertTableHandle = (StorageInsertTableHandle) insertTableHandle;
        FtpConfig ftpConfig = storageInsertTableHandle.getFtpConfig();
        log.debug("pageSinkId :" + pageSinkId.getId());
        log.debug("ftpConfig :" + ftpConfig.toString());
        log.debug("session :" + session.getQueryId());
        ftpConfig.setNodeId(nodeConfig.getNodeId() + Constant.UNDERLINE + session.getQueryId());
        return new StoragePageSink(storageClient, storageInsertTableHandle.getStorageTableHandle(),
                storageInsertTableHandle.getStorageTable(), storageInsertTableHandle.getColumntypes(),
                storageInsertTableHandle.getColumnNames(), ftpConfig);
    }

}
