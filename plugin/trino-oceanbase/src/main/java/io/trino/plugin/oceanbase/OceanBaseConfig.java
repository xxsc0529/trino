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
package io.trino.plugin.oceanbase;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import jakarta.validation.constraints.Min;

import java.util.concurrent.TimeUnit;

public class OceanBaseConfig
{
    private OceanBaseCompatibleMode compatibleMode = OceanBaseCompatibleMode.MySQL;
    private boolean autoReconnect = true;
    private int maxReconnects = 3;
    private Duration connectionTimeout = new Duration(10, TimeUnit.SECONDS);
    private boolean driverUseInformationSchema = true;
    private boolean remarksReportingEnabled;

    public OceanBaseCompatibleMode getCompatibleMode()
    {
        return compatibleMode;
    }

    @Config("oceanbase.compatible-mode")
    public OceanBaseConfig setCompatibleMode(String compatibleMode)
    {
        this.compatibleMode = OceanBaseCompatibleMode.parse(compatibleMode);
        return this;
    }

    public boolean isAutoReconnect()
    {
        return autoReconnect;
    }

    @Config("oceanbase.auto-reconnect")
    public OceanBaseConfig setAutoReconnect(boolean autoReconnect)
    {
        this.autoReconnect = autoReconnect;
        return this;
    }

    @Min(1)
    public int getMaxReconnects()
    {
        return maxReconnects;
    }

    @Config("oceanbase.max-reconnects")
    public OceanBaseConfig setMaxReconnects(int maxReconnects)
    {
        this.maxReconnects = maxReconnects;
        return this;
    }

    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("oceanbase.connection-timeout")
    public OceanBaseConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public boolean isDriverUseInformationSchema()
    {
        return driverUseInformationSchema;
    }

    @Config("oceanbase.use-information-schema")
    @ConfigDescription("Value of JDBC driver connection property 'useInformationSchema' on MySQL mode")
    public OceanBaseConfig setDriverUseInformationSchema(boolean driverUseInformationSchema)
    {
        this.driverUseInformationSchema = driverUseInformationSchema;
        return this;
    }

    public boolean isRemarksReportingEnabled()
    {
        return remarksReportingEnabled;
    }

    @Config("oceanbase.remarks-reporting.enabled")
    public OceanBaseConfig setRemarksReportingEnabled(boolean enabled)
    {
        this.remarksReportingEnabled = enabled;
        return this;
    }
}
