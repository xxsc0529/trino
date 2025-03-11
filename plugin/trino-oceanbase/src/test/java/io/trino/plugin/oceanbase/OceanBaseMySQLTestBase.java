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


import com.github.dockerjava.api.model.ContainerNetwork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.oceanbase.OceanBaseCEContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

public abstract class OceanBaseMySQLTestBase extends OceanBaseTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseMySQLTestBase.class);

    private static final int SQL_PORT = 2881;
    private static final int RPC_PORT = 2882;

    private static final String CLUSTER_NAME = "trino-oceanbase-ci";
    private static final String TEST_TENANT = "trino";
    private static final String SYS_PASSWORD = "123456";
    private static final String TEST_PASSWORD = "654321";

    public static final Network NETWORK = Network.newNetwork();

    public static final OceanBaseCEContainer CONTAINER =
            new OceanBaseCEContainer("oceanbase/oceanbase-ce:latest")
                    .withNetwork(NETWORK)
                    .withMode(OceanBaseCEContainer.Mode.MINI)
                    .withTenantName(TEST_TENANT)
                    .withPassword(TEST_PASSWORD)
                    .withEnv("OB_CLUSTER_NAME", CLUSTER_NAME)
                    .withEnv("OB_SYS_PASSWORD", SYS_PASSWORD)
                    .withEnv("OB_DATAFILE_SIZE", "2G")
                    .withEnv("OB_LOG_DISK_SIZE", "4G")
                    .withStartupTimeout(Duration.ofMinutes(4));


    public static String getContainerIP(GenericContainer<?> container) {
        String ip =
                container.getContainerInfo().getNetworkSettings().getNetworks().values().stream()
                        .findFirst()
                        .map(ContainerNetwork::getIpAddress)
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "Can't get IP address of container: " + container));
        LOG.info("Docker image: {}, container IP: {}", container.getDockerImageName(), ip);
        return ip;
    }

    private static String obServerIP;
    private static OceanBaseUserInfo userInfo;


    public static String getOBServerIP() {
        if (obServerIP == null) {
            obServerIP = getContainerIP(CONTAINER);
        }
        return obServerIP;
    }

    public static OceanBaseUserInfo getUserInfo() {
        if (userInfo == null) {
            userInfo = OceanBaseUserInfo.parse(CONTAINER.getUsername());
        }
        return userInfo;
    }

    public static Connection getSysJdbcConnection() throws SQLException {
        String jdbcUrl =
                "jdbc:mysql://"
                        + CONTAINER.getHost()
                        + ":"
                        + CONTAINER.getMappedPort(SQL_PORT)
                        + "/?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
        return DriverManager.getConnection(jdbcUrl, "root", SYS_PASSWORD);
    }

    public static String getSysParameter(String parameter) {
        try (Connection connection = getSysJdbcConnection();
                Statement statement = connection.createStatement()) {
            String sql = String.format("SHOW PARAMETERS LIKE '%s'", parameter);
            ResultSet rs = statement.executeQuery(sql);
            if (rs.next()) {
                return rs.getString("VALUE");
            }
            throw new RuntimeException("Parameter '" + parameter + "' not found");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void createSysUser(String user, String password) throws SQLException {
        assert user != null && password != null;
        try (Connection connection = getSysJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE USER '" + user + "' IDENTIFIED BY '" + password + "'");
            statement.execute("GRANT ALL PRIVILEGES ON *.* TO '" + user + "'@'%'");
        }
    }

    @Override
    public String getHost() {
        return CONTAINER.getHost();
    }

    @Override
    public int getPort() {
        return CONTAINER.getMappedPort(SQL_PORT);
    }

    @Override
    public int getRpcPort() {
        return CONTAINER.getMappedPort(RPC_PORT);
    }

    @Override
    public String getJdbcUrl() {
        return "jdbc:mysql://"
                + getHost()
                + ":"
                + getPort()
                + "/"
                + getSchemaName()
                + "?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
    }

    @Override
    public String getClusterName() {
        return CLUSTER_NAME;
    }

    @Override
    public String getSchemaName() {
        return CONTAINER.getDatabaseName();
    }

    @Override
    public String getSysUsername() {
        return "root";
    }

    @Override
    public String getSysPassword() {
        return SYS_PASSWORD;
    }

    @Override
    public String getUsername() {
        return CONTAINER.getUsername();
    }

    @Override
    public String getPassword() {
        return CONTAINER.getPassword();
    }
}
