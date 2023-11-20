package org.ebyhr.trino.storage.catalog;

import io.airlift.log.Logger;
import io.trino.connector.CatalogProperties;
import org.ebyhr.trino.storage.StorageColumnHandle;
import org.ebyhr.trino.storage.StorageConfig;
import org.ebyhr.trino.storage.dto.FtpConfig;
import org.ebyhr.trino.storage.utils.Utils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JdbcConnector
{
    public static final Logger log = Logger.get(JdbcConnector.class);

    public static List<StorageColumnHandle> getColumnHandle(FtpConfig ftpConfig, CatalogMate catalogMate)
    {
        CatalogProperties catalog = Utils.getCatalog(ftpConfig.getCatalog(), catalogMate);
        Map<String, String> properties = catalog.getProperties();

        List<StorageColumnHandle> storageColumnHandles = new ArrayList<>();
        String url = properties.get("connection-url");
        String user = properties.get("connection-user");
        String password = properties.get("connection-password");
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection connection = DriverManager.getConnection(url, user, password);
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet rs = metaData.getColumns(null, ftpConfig.getDatabase(), ftpConfig.getTable(), "%");
            while (rs.next()) {
                String columnType = rs.getString("TYPE_NAME");
                String columnName = rs.getString("COLUMN_NAME");
                int precision = rs.getInt("COLUMN_SIZE");
                int scale = rs.getInt("DECIMAL_DIGITS");
                StorageColumnHandle storageColumnHandle = new StorageColumnHandle(columnName,
                        Utils.matchType(columnType, precision, scale));

                log.info(columnName + " " + columnType + " " + precision + " " + scale);
                storageColumnHandles.add(storageColumnHandle);
                log.info("storageColumnHandle :" + storageColumnHandle.toString());
            }

        }
        catch (Exception e) {

        }
        return storageColumnHandles;
    }
}
