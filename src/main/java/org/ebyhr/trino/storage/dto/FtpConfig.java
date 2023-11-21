package org.ebyhr.trino.storage.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class FtpConfig
{
    private String ftpHost;
    private String ftpUser;
    private String ftpPassWord;
    private String ftpPort;

    // file type
    private String schema;

    // file  path
    private String path;


    private String catalog;
    private String database;
    private String table;

    private List<String> column;

    private String nodeId;

    @JsonCreator
    public FtpConfig()
    {
    }

    @JsonProperty
    public String getFtpHost()
    {
        return ftpHost;
    }

    @JsonProperty
    public void setFtpHost(String ftpHost)
    {
        this.ftpHost = ftpHost;
    }

    @JsonProperty
    public String getFtpUser()
    {
        return ftpUser;
    }

    @JsonProperty
    public void setFtpUser(String ftpUser)
    {
        this.ftpUser = ftpUser;
    }

    @JsonProperty
    public String getFtpPassWord()
    {
        return ftpPassWord;
    }

    @JsonProperty
    public void setFtpPassWord(String ftpPassWord)
    {
        this.ftpPassWord = ftpPassWord;
    }

    @JsonProperty
    public String getFtpPort()
    {
        return ftpPort;
    }

    @JsonProperty
    public void setFtpPort(String ftpPort)
    {
        this.ftpPort = ftpPort;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public void setSchema(String schema)
    {
        this.schema = schema;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public void setPath(String path)
    {
        this.path = path;
    }

    @JsonProperty
    public String getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public void setCatalog(String catalog)
    {
        this.catalog = catalog;
    }

    @JsonProperty
    public String getDatabase()
    {
        return database;
    }

    @JsonProperty
    public void setDatabase(String database)
    {
        this.database = database;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }
    @JsonProperty
    public void setTable(String table)
    {
        this.table = table;
    }
    @JsonProperty
    public List<String> getColumn()
    {
        return column;
    }
    @JsonProperty
    public void setColumn(List<String> column)
    {
        this.column = column;
    }

    @JsonProperty
    public String getNodeId()
    {
        return nodeId;
    }
    @JsonProperty
    public void setNodeId(String nodeId)
    {
        this.nodeId = nodeId;
    }

    @Override
    public String toString()
    {
        return "FtpConfig{" + "ftpHost='" + ftpHost + '\'' + ", ftpUser='" + ftpUser + '\'' + ", ftpPassWord='" + ftpPassWord + '\'' + ", ftpPort='" + ftpPort + '\'' + ", schema='" + schema + '\'' + ", path='" + path + '\'' + ", catalog='" + catalog + '\'' + ", database='" + database + '\'' + ", table='" + table + '\'' + ", column=" + column + ", nodeId='" + nodeId + '\'' + '}';
    }
}
