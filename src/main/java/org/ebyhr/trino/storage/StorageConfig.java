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

import java.util.List;

public class StorageConfig
{
    private boolean allowLocalFiles = true;


    private String ftpHost = "192.168.31.129";
    private String ftpUser = "root";
    private String ftpPassWord = "root";
    private String ftpPort = "21";

    // file type
    private String schema;

    // file  path
    private String path;


    private String catalog;
    private String database;
    private String table;

    private List<String> column;


    public boolean getAllowLocalFiles()
    {
        return allowLocalFiles;
    }

    public String getFtpHost()
    {
        return ftpHost;
    }

    public String getFtpUser()
    {
        return ftpUser;
    }

    public String getFtpPassWord()
    {
        return ftpPassWord;
    }

    public String getFtpPort()
    {
        return ftpPort;
    }

    public String getPath()
    {
        return path;
    }

    @Config("allow-local-files")
    @ConfigDescription("If true, allow reading local files")
    public void setAllowLocalFiles(boolean allowLocalFiles)
    {
        this.allowLocalFiles = allowLocalFiles;
    }


    public void setFtpHost(String ftpHost)
    {
        this.ftpHost = ftpHost;
    }


    public void setFtpPort(String ftpPort)
    {
        this.ftpPort = ftpPort;
    }


    public void setFtpUser(String ftpUser)
    {
        this.ftpUser = ftpUser;
    }


    public void setFtpPassWord(String ftpPassWord)
    {
        this.ftpPassWord = ftpPassWord;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    public String getCatalog()
    {
        return catalog;
    }

    public void setCatalog(String catalog)
    {
        this.catalog = catalog;
    }

    public String getSchema()
    {
        return schema;
    }

    public void setSchema(String schema)
    {
        this.schema = schema;
    }

    public String getTable()
    {
        return table;
    }

    public void setTable(String table)
    {
        this.table = table;
    }

    public List<String> getColumn()
    {
        return column;
    }

    public void setColumn(List<String> column)
    {
        this.column = column;
    }

    public String getDatabase()
    {
        return database;
    }

    public void setDatabase(String database)
    {
        this.database = database;
    }
}
