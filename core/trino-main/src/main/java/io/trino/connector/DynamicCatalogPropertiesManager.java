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
package io.trino.connector;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.trino.connector.CatalogHandle.createRootCatalogHandle;

public class DynamicCatalogPropertiesManager
        extends StaticCatalogManager
{
    private static final Logger log = Logger.get(DynamicCatalogPropertiesManager.class);
    private final Duration watchTimeout;
    private final boolean dynamicUpdateEanbled;
    private final File catalogConfigurationDir;
    private final ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("catalog-properios-update-thread-%d").build());

    @Inject
    public DynamicCatalogPropertiesManager(CatalogFactory catalogFactory, DynamicCatalogPropertiesConfig config)
    {
        super(catalogFactory, config);
        watchTimeout = config.getWatchTimeout();
        dynamicUpdateEanbled = config.isDynamicUpdateEanbled();
        catalogConfigurationDir = config.getCatalogConfigurationDir();
    }

    public void loadInitialCatalogs()
    {
        super.loadInitialCatalogs();
        if (dynamicUpdateEanbled) {
            singleThreadExecutor.submit(() -> {
                try {
                    log.info("-- Catalog watcher thread start --");
                    startCatalogConfigWatcher(catalogConfigurationDir);
                }
                catch (Exception e) {
                    log.error(e);
                }
            });
        }
    }

    private void startCatalogConfigWatcher(File catalogConfigurationDir) throws IOException, InterruptedException
    {
        WatchService watchService = FileSystems.getDefault().newWatchService();
        Paths.get(catalogConfigurationDir.getAbsolutePath()).register(
                watchService,
                StandardWatchEventKinds.ENTRY_MODIFY,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_DELETE);

        while (true) {
            WatchKey key = watchService.take();
            for (WatchEvent<?> event : key.pollEvents()) {
                if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
                    log.info("New file in catalog directory : " + event.context());
                    Path newCatalog = (Path) event.context();
                    addCatalog(newCatalog);
                }
                else if (event.kind() == StandardWatchEventKinds.ENTRY_DELETE) {
                    log.info("Delete file from catalog directory : " + event.context());
                    Path deletedCatalog = (Path) event.context();
                    deleteCatalog(deletedCatalog);
                }
                else if (event.kind() == StandardWatchEventKinds.ENTRY_MODIFY) {
                    log.info("Modify file from catalog directory : " + event.context());
                    Path modifiedCatalog = (Path) event.context();
                    modifyCatalog(modifiedCatalog);
                }
            }
            boolean valid = key.reset();
            if (!valid) {
                break;
            }
        }
    }

    private void addCatalog(Path catalogPath)
    {
        File file = new File(catalogConfigurationDir, catalogPath.getFileName().toString());
        if (file.isFile() && file.getName().endsWith(".properties")) {
            try {
                TimeUnit.SECONDS.sleep((long) watchTimeout.getValue(TimeUnit.SECONDS));
                String catalogName = Files.getNameWithoutExtension(file.getName());
                Map<String, String> properties;
                try {
                    properties = new HashMap<>(loadPropertiesFrom(file.getPath()));
                }
                catch (IOException e) {
                    throw new UncheckedIOException("Error reading catalog property file " + file, e);
                }

                String connectorName = properties.remove("connector.name");
                checkState(connectorName != null, "Catalog configuration %s does not contain connector.name", file.getAbsoluteFile());

                CatalogProperties catalogProperties = new CatalogProperties(createRootCatalogHandle(catalogName), connectorName, ImmutableMap.copyOf(properties));
                addCatalogProperties(catalogProperties);
                loadCatalogByName(catalogProperties);
            }
            catch (Exception e) {
                log.error(e);
            }
        }
    }

    private void deleteCatalog(Path catalogPath)
    {
        if (catalogPath.getFileName().toString().endsWith(".properties")) {
            String catalogName = Files.getNameWithoutExtension(catalogPath.getFileName().toString());
            log.info("-- Removing catalog %s", catalogName);
            deleteCatalogs(catalogName);
            log.info("-- Removed catalog %s", catalogName);
        }
    }

    private void modifyCatalog(Path catalogPath)
    {
        deleteCatalog(catalogPath);
        addCatalog(catalogPath);
    }
}
