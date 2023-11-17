package org.ebyhr.trino.storage.catalog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.connector.CatalogProperties;
import io.trino.connector.ConnectorName;
import io.trino.connector.StaticCatalogManagerConfig;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.spi.connector.CatalogHandle;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.trino.spi.connector.CatalogHandle.createRootCatalogHandle;

public class CatalogMate
{

    public static final Logger log = Logger.get(JdbcConnector.class);


    private final List<CatalogProperties> catalogProperties;

    @Inject
    public CatalogMate(StaticCatalogManagerConfig config)
    {
        List<String> disabledCatalogs = firstNonNull(config.getDisabledCatalogs(), ImmutableList.of());

        ImmutableList.Builder<CatalogProperties> catalogProperties = ImmutableList.builder();
        for (File file : listCatalogFiles(config.getCatalogConfigurationDir())) {
            String catalogName = Files.getNameWithoutExtension(file.getName());
            checkArgument(!catalogName.equals(GlobalSystemConnector.NAME), "Catalog name SYSTEM is reserved for " +
                    "internal usage");
            if (disabledCatalogs.contains(catalogName)) {
                log.info("Skipping disabled catalog %s", catalogName);
                continue;
            }

            Map<String, String> properties;
            try {
                properties = new HashMap<>(loadPropertiesFrom(file.getPath()));
            }
            catch (IOException e) {
                throw new UncheckedIOException("Error reading catalog property file " + file, e);
            }

            String connectorName = properties.remove("connector.name");
            checkState(connectorName != null, "Catalog configuration %s does not contain connector.name",
                    file.getAbsoluteFile());
            if (connectorName.indexOf('-') >= 0) {
                String deprecatedConnectorName = connectorName;
                connectorName = connectorName.replace('-', '_');
                log.warn("Catalog '%s' is using the deprecated connector name '%s'. The correct connector name is " +
                        "'%s'", catalogName, deprecatedConnectorName, connectorName);
            }

            catalogProperties.add(new CatalogProperties(createRootCatalogHandle(catalogName,
                    new CatalogHandle.CatalogVersion("default")), new ConnectorName(connectorName),
                    ImmutableMap.copyOf(properties)));
        }
        this.catalogProperties = catalogProperties.build();
    }

    private static List<File> listCatalogFiles(File catalogsDirectory)
    {
        if (catalogsDirectory == null || !catalogsDirectory.isDirectory()) {
            return ImmutableList.of();
        }

        File[] files = catalogsDirectory.listFiles();
        if (files == null) {
            return ImmutableList.of();
        }
        return Arrays.stream(files).filter(File::isFile).filter(file -> file.getName().endsWith(".properties")).collect(toImmutableList());
    }

    public List<CatalogProperties> getCatalogProperties()
    {
        return catalogProperties;
    }
}
