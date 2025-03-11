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

import java.util.Locale;

public enum OceanBaseCompatibleMode
{
    MySQL,
    Oracle;

    public static OceanBaseCompatibleMode parse(String text)
    {
        if (text == null || text.trim().isEmpty()) {
            return OceanBaseCompatibleMode.MySQL;
        }
        return switch (text.trim().toLowerCase(Locale.ENGLISH)) {
            case "mysql" -> OceanBaseCompatibleMode.MySQL;
            case "oracle" -> OceanBaseCompatibleMode.Oracle;
            default -> throw new IllegalArgumentException("Unsupported compatible mode: " + text);
        };
    }

    public boolean isMySQLMode()
    {
        return this == MySQL;
    }

    @Override
    public String toString()
    {
        return this.name().toLowerCase(Locale.ENGLISH);
    }
}
