/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ebyhr.trino.storage;

import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.log.Logger;
import io.trino.connector.StaticCatalogManagerConfig;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.VarcharType;
import org.ebyhr.trino.storage.catalog.CatalogMate;
import org.ebyhr.trino.storage.catalog.JdbcConnector;
import org.ebyhr.trino.storage.dto.FtpConfig;
import org.ebyhr.trino.storage.operator.FilePlugin;
import org.ebyhr.trino.storage.operator.PluginFactory;
import org.ebyhr.trino.storage.utils.Utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.ebyhr.trino.storage.ByteResponseHandler.createByteResponseHandler;
import static org.ebyhr.trino.storage.ptf.ListTableFunction.LIST_SCHEMA_NAME;

public class StorageClient
{
    private static final Logger log = Logger.get(StorageClient.class);

    private final TrinoFileSystemFactory fileSystemFactory;
    private final HttpClient httpClient;
    private final boolean allowLocalFiles;

    private final StorageConfig storageConfig;
    private final StaticCatalogManagerConfig staticCatalogManagerConfig;
    private final CatalogMate catalogMate;


    @Inject
    public StorageClient(TrinoFileSystemFactory fileSystemFactory, @ForStorage HttpClient httpClient,
                         StorageConfig storageConfig, StaticCatalogManagerConfig staticCatalogManagerConfig,
                         CatalogMate catalogMate)
    {

        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.allowLocalFiles = requireNonNull(storageConfig, "storageConfig is null").getAllowLocalFiles();
        this.storageConfig = requireNonNull(storageConfig, "storageConfig is null");
        this.staticCatalogManagerConfig = requireNonNull(staticCatalogManagerConfig, "storageConfig is null");
        this.catalogMate = requireNonNull(catalogMate, "storageConfig is null");

    }

    public List<String> getSchemaNames()
    {
        return Stream.of(FileType.values()).map(FileType::toString).collect(Collectors.toList());
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        return new HashSet<>();
    }

    public StorageTable getTable(ConnectorSession session, String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");

        if (isLocalFile(tableName) && !allowLocalFiles) {
            throw new TrinoException(PERMISSION_DENIED, "Reading local files is disabled");
        }
        if (schema.equals(LIST_SCHEMA_NAME)) {
            return new StorageTable(StorageSplit.Mode.LIST, tableName, List.of(new StorageColumnHandle("path",
                    VarcharType.VARCHAR)));
        }

        FilePlugin plugin = PluginFactory.create(schema);
        try {
            List<StorageColumnHandle> columns = null;
            if (tableName.startsWith("ftp://") || tableName.startsWith("FTP://")) {
//                log.info("=================" + staticCatalogManagerConfig.getCatalogConfigurationDir());
//                log.info("=================catalogConnect " + catalogMate.getCatalogProperties().size());
                FtpConfig ftpConfig = Utils.ftpAnalyze(schema,tableName);
                columns = JdbcConnector.getColumnHandle(ftpConfig, catalogMate);
            }
            else {
                columns = plugin.getFields(tableName, path -> getInputStream(session, path), storageConfig);
            }
            return new StorageTable(StorageSplit.Mode.TABLE, tableName, columns);
        }
        catch (Exception e) {
            log.error(e, "Failed to get table: %s.%s", schema, tableName);
            return null;
        }
    }

    private boolean isLocalFile(String path)
    {
        return path.startsWith("file:") || !(path.startsWith("http://") || path.startsWith("https://") || path.startsWith("hdfs://") || path.startsWith("s3a://") || path.startsWith("s3://"));
    }

    public InputStream getInputStream(ConnectorSession session, String path)
    {
        try {
            if (path.startsWith("http://") || path.startsWith("https://")) {
                Request request = prepareGet().setUri(URI.create(path)).build();
                ByteResponseHandler.ByteResponse response = httpClient.execute(request, createByteResponseHandler());
                int status = response.getStatusCode();
                if (status != HttpStatus.OK.code()) {
                    throw new IllegalStateException(format("Request to '%s' returned unexpected status code: '%d'",
                            path, status));
                }
                return new ByteArrayInputStream(response.getBody());
            }
            if (path.startsWith("hdfs://") || path.startsWith("s3a://") || path.startsWith("s3://")) {
                return fileSystemFactory.create(session).newInputFile(Location.of(path)).newStream();
            }

            if (!allowLocalFiles) {
                throw new TrinoException(PERMISSION_DENIED, "Reading local files is disabled");
            }
            return URI.create(path).toURL().openStream();
        }
        catch (IOException e) {
            throw new UncheckedIOException(format("Failed to open stream for %s", path), e);
        }
    }

    public FileIterator list(ConnectorSession session, String path)
    {
        try {
            if (path.startsWith("http://") || path.startsWith("https://")) {
                throw new IllegalArgumentException("Listing files over HTTP is not supported");
            }
            if (path.startsWith("hdfs://") || path.startsWith("s3a://") || path.startsWith("s3://")) {
                return fileSystemFactory.create(session).listFiles(Location.of(path));
            }
            if (!allowLocalFiles) {
                throw new TrinoException(PERMISSION_DENIED, "Reading local files is disabled");
            }
            if (path.startsWith("file://")) {
                path = path.substring("file://".length());
            }
            else if (path.startsWith("file:")) {
                path = path.substring("file:".length());
            }
            return new LocalFileSystem(Path.of(path)).listFiles(Location.of("local:///"));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public StorageConfig getStorageConfig()
    {
        return storageConfig;
    }
}
