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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

public class TestingOceanBaseServer extends OceanBaseMySQLTestBase
        implements AutoCloseable
{

    private static final Logger LOG = LoggerFactory.getLogger(TestingOceanBaseServer.class);

    public TestingOceanBaseServer()
    {
        CONTAINER.withLogConsumer(new Slf4jLogConsumer(LOG)).start();
    }

    public TestingOceanBaseServer(ZoneId zoneId)
    {
        CONTAINER
                .withEnv("TZ",zoneId.getId())
                .withLogConsumer(new Slf4jLogConsumer(LOG)).start();
    }


    public void execute(String sql)
    {
        try (Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection createConnection()
            throws SQLException
    {
        return CONTAINER.createConnection("");
    }

    public String getUsername()
    {
        return super.getUsername();
    }

    public String getPassword()
    {
        return super.getPassword();
    }

    public String getDatabaseName()
    {
        return CONTAINER.getDatabaseName();
    }

    public String getJdbcUrl()
    {
        return CONTAINER.getJdbcUrl();
    }

    @Override
    public void close()
    {
        CONTAINER.close();
    }
}
