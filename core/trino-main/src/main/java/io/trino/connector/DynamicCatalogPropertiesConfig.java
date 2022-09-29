package io.trino.connector;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

public class DynamicCatalogPropertiesConfig extends StaticCatalogManagerConfig {
    private Duration watchTimeout = new Duration(5, TimeUnit.SECONDS);
    private boolean dynamicUpdateEanbled;

    public Duration getWatchTimeout() {
        return watchTimeout;
    }

    @Config("catalog.watch-timeout")
    public DynamicCatalogPropertiesConfig setWatchTimeout(Duration watchTimeout) {
        this.watchTimeout = watchTimeout;
        return this;
    }

    public boolean isDynamicUpdateEanbled() {
        return dynamicUpdateEanbled;
    }

    @Config("catalog.dynamic-update.enabled")
    public DynamicCatalogPropertiesConfig setDynamicUpdateEanbled(boolean dynamicUpdateEanbled) {
        this.dynamicUpdateEanbled = dynamicUpdateEanbled;
        return this;
    }
}
