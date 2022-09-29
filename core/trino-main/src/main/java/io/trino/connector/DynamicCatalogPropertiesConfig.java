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

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

public class DynamicCatalogPropertiesConfig
        extends StaticCatalogManagerConfig
{
    private Duration watchTimeout = new Duration(5, TimeUnit.SECONDS);
    private boolean dynamicUpdateEanbled;

    public Duration getWatchTimeout()
    {
        return watchTimeout;
    }

    @Config("catalog.watch-timeout")
    public DynamicCatalogPropertiesConfig setWatchTimeout(Duration watchTimeout)
    {
        this.watchTimeout = watchTimeout;
        return this;
    }

    public boolean isDynamicUpdateEanbled()
    {
        return dynamicUpdateEanbled;
    }

    @Config("catalog.dynamic-update.enabled")
    public DynamicCatalogPropertiesConfig setDynamicUpdateEanbled(boolean dynamicUpdateEanbled)
    {
        this.dynamicUpdateEanbled = dynamicUpdateEanbled;
        return this;
    }
}
