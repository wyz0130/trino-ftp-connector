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

package org.ebyhr.trino.storage;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class StorageConfig
{
    private boolean allowLocalFiles = true;

    private String separator = ",";

    private String format = "json";

    private char columnSeparator;

    public boolean getAllowLocalFiles()
    {
        return allowLocalFiles;
    }


    @Config("allow-local-files")
    @ConfigDescription("If true, allow reading local files")
    public void setAllowLocalFiles(boolean allowLocalFiles)
    {
        this.allowLocalFiles = allowLocalFiles;
    }


    public String getSeparator()
    {
        return separator;
    }

    @Config("column_separator")
    public void setSeparator(String separator)
    {
        this.separator = separator;
    }

    public String getFormat()
    {
        return format;
    }

    @Config("format")
    public void setFormat(String format)
    {
        this.format = format;
    }

    public char getColumnSeparator()
    {
        return columnSeparator;
    }

    public void setColumnSeparator(char columnSeparator)
    {
        this.columnSeparator = columnSeparator;
    }

    @Override
    public String toString()
    {
        return "StorageConfig{" + "allowLocalFiles=" + allowLocalFiles + ", separator='" + separator + '\'' + ", " +
                "format='" + format + '\'' + ", columnSeparator=" + columnSeparator + '}';
    }
}
