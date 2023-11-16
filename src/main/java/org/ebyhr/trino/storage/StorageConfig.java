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


    private String ftpHost = "192.168.31.129";
    private String ftpUser = "root";
    private String ftpPassWord = "root";
    private String ftpPort = "21";

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

    @Config("allow-local-files")
    @ConfigDescription("If true, allow reading local files")
    public StorageConfig setAllowLocalFiles(boolean allowLocalFiles)
    {
        this.allowLocalFiles = allowLocalFiles;
        return this;
    }


    @Config("ftp_host")
    public void setFtpHost(String ftpHost)
    {
        this.ftpHost = ftpHost;
    }

    @Config("ftp_port")
    public void setFtpPort(String ftpPort)
    {
        this.ftpPort = ftpPort;
    }

    @Config("ftp_user")
    public void setFtpUser(String ftpUser)
    {
        this.ftpUser = ftpUser;
    }

    @Config("ftp_password")
    public void setFtpPassWord(String ftpPassWord)
    {
        this.ftpPassWord = ftpPassWord;
    }


}
