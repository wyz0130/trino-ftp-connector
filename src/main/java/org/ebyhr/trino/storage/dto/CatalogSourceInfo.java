package org.ebyhr.trino.storage.dto;

import java.util.List;

public class CatalogSourceInfo
{
    private String source;

    private List<String> column;

    private List<String> exclude;

    public String getSource()
    {
        return source;
    }

    public void setSource(String source)
    {
        this.source = source;
    }

    public List<String> getColumn()
    {
        return column;
    }

    public void setColumn(List<String> column)
    {
        this.column = column;
    }

    public List<String> getExclude()
    {
        return exclude;
    }

    public void setExclude(List<String> exclude)
    {
        this.exclude = exclude;
    }

    @Override
    public String toString()
    {
        return "CatalogSourceInfo{" + "source='" + source + '\'' + ", column=" + column + ", exclude=" + exclude + '}';
    }
}
