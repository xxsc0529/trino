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

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.security.Identity;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestCredentialPassthrough
{
    private TestingOceanBaseServer oceanbaseServer;
    private QueryRunner queryRunner;

    @Test
    public void testCredentialPassthrough()
    {
        queryRunner.execute(getSession(oceanbaseServer), "CREATE TABLE test_create (a bigint, b double, c varchar)");
    }

    @BeforeAll
    public void createQueryRunner()
            throws Exception
    {
        oceanbaseServer = new TestingOceanBaseServer();
        queryRunner = OceanBaseQueryRunner.builder(oceanbaseServer)
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("connection-url", oceanbaseServer.getJdbcUrl())
                        .put("user-credential-name", "mysql.user")
                        .put("password-credential-name", "mysql.password")
                        .buildOrThrow())
                .build();
    }

    @AfterAll
    public final void destroy()
    {
        queryRunner.close();
        queryRunner = null;
        oceanbaseServer.close();
        oceanbaseServer = null;
    }

    private static Session getSession(TestingOceanBaseServer oceanbaseServer)
    {
        Map<String, String> extraCredentials = ImmutableMap.of("mysql.user", oceanbaseServer.getUsername(), "mysql.password", oceanbaseServer.getPassword());
        return testSessionBuilder()
                .setCatalog("mysql")
                .setSchema(oceanbaseServer.getDatabaseName())
                .setIdentity(Identity.forUser(oceanbaseServer.getUsername())
                        .withExtraCredentials(extraCredentials)
                        .build())
                .build();
    }
}
