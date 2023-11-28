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
package org.ebyhr.trino.storage.utils;


import io.airlift.log.Logger;
import io.trino.connector.CatalogProperties;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.commons.io.FileUtils;
import org.ebyhr.trino.storage.catalog.CatalogMate;
import org.ebyhr.trino.storage.dto.FtpConfig;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static io.trino.spi.type.VarbinaryType.VARBINARY;


public class Utils
{

    public static final Logger log = Logger.get(Utils.class);

    public static final HashSet<String> FLIP_SYMBOL_TYPE = new HashSet<>(Arrays.asList(StandardTypes.BIGINT,
            StandardTypes.INTEGER, StandardTypes.TINYINT, StandardTypes.SMALLINT, StandardTypes.DOUBLE));

    public static FtpConfig ftpAnalyze(String schemaName, String tableName)
    {

        FtpConfig ftpConfig = new FtpConfig();
        ftpConfig.setSchema(schemaName);
        // ftp://root:root@ 127.0.0.1:21/data?schema=doris.demo.table2{v1,v2}
        log.debug("ftpAnalyze path :" + tableName);
        if (tableName.contains("{") && tableName.contains("}")) {
            String columns = tableName.substring(tableName.indexOf("{"), tableName.indexOf("}") + 1);
            tableName = tableName.replace(columns, "");
            columns = columns.substring(1, columns.length() - 1);
            String[] split = columns.split(",");
            ftpConfig.setColumn(Arrays.asList(split));
            tableName.replace(columns, "");
        }
        if (tableName.startsWith(Constant.FTP)) {
            tableName = tableName.replace(Constant.FTP, Constant.ftp);
        }
        //root:root@127.0.0.1:21/data?schema=doris.demo.table2
        tableName = tableName.replace(Constant.ftp, "").trim();
        String[] pathSplit = tableName.split("@");
        //root:root
        String[] usrPwd = pathSplit[0].split(":");
        ftpConfig.setFtpUser(usrPwd[0]);
        ftpConfig.setFtpPassWord(usrPwd[1]);

        //127.0.0.1:21/data?schema=doris.demo.table2
        pathSplit = pathSplit[1].split("\\?");
        String catalogMate = pathSplit[1].replace("schema=", "").trim();
        String[] catalogArray = catalogMate.split("\\.");
        ftpConfig.setCatalog(catalogArray[0]);
        ftpConfig.setDatabase(catalogArray[1]);
        ftpConfig.setTable(catalogArray[2]);


        //127.0.0.1:21/data
        pathSplit = pathSplit[0].split("/");
        String[] ipPort = pathSplit[0].split(":");
        ftpConfig.setFtpHost(ipPort[0]);
        ftpConfig.setFtpPort(ipPort[1]);

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("/");
        for (int i = 1; i < pathSplit.length; i++) {
            stringBuilder.append(pathSplit[i]);
            stringBuilder.append("/");
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        ftpConfig.setPath(stringBuilder.toString());
        return ftpConfig;
    }

    public static String fileName(String path)
    {
        String[] split = path.split("/");
        for (String s : split) {
            log.debug("split :" + s);
        }
        path = split[split.length - 1];
        String[] split1 = path.split("\\..");
        return split1[0];
    }

    /**
     * Read table json from metaDir by schema name and table name
     *
     * @param tableName table name
     * @param metaDir   meta dir
     * @return json file content
     */
    public static String readTableJson(String tableName, String metaDir)
    {
        try {
            String tableMetaPath = metaDir + File.separator + tableName + Constant.TABLE_META_FILE_TAIL;
            log.debug("tableMetaPath : " + tableMetaPath);
            return FileUtils.readFileToString(new File(tableMetaPath), Constant.JSON_ENCODING_UTF8);
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return "";
    }


    /**
     * Read table json from metaDir by schema name and table name.
     * And convert it to an Object of TableMetaInfo.
     *
     * @param tableName table name
     * @param metaDir   meta info dir
     * @return Object of TableMetaInfo
     */
    public static String getTableMetaInfoFromJson(String tableName, String metaDir)
    {
        long startTime = System.currentTimeMillis();
        try {
            String jsonString = readTableJson(tableName, metaDir);
            log.debug("getTableMetaInfoFromJson" + jsonString);
            return jsonString;
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        finally {
            log.info(String.format("Read meta info of TABLE.%s from json, totally used %d ms.", tableName,
                    (System.currentTimeMillis() - startTime)));
        }
        return null;
    }


    /**
     * Find the presto type of column you configured in json file by type flag.
     *
     * @param type The type value that configured in json file.
     * @return type in presto
     */
    public static Type matchType(String type, Integer precision, Integer scale)
    {
        log.debug("type :" + type);
        if (type == null) {
            return VarcharType.VARCHAR;
        }
        switch (type.toLowerCase()) {
            /*case "string":
                return VarcharType.VARCHAR;*/
            case "varbinary":
                return VARBINARY;
            case "int":
                return IntegerType.INTEGER;
            case "integer":
                return IntegerType.INTEGER;
            case "bigint":
                return BigintType.BIGINT;
            case "double":
                return DoubleType.DOUBLE;
            case "boolean":
                return BooleanType.BOOLEAN;
            case "array<string>":
                return new ArrayType(VarcharType.VARCHAR);
            case "date":
                return DateType.DATE;
            case "timestamp":
            case "datetime":
                return TimestampType.TIMESTAMP_MILLIS;
            case "decimal":
                return DecimalType.createDecimalType(precision, scale);
            case "number":
                return DecimalType.createDecimalType(Constant.DECIMAL_DEFAULT_PRECISION,
                        Constant.DECIMAL_DEFAULT_SCALE);
            default:
                return VarcharType.VARCHAR;
        }
    }


    /**
     * Copy contents in ${srcAry} from position ${srcPos} for ${length} bytes.
     *
     * @param srcAry source array
     * @param srcPos start position
     * @param length length
     * @return copied byte array
     */
    public static byte[] arrayCopy(byte[] srcAry, int srcPos, int length)
    {
        byte[] destAry = new byte[length];
        System.arraycopy(srcAry, srcPos, destAry, 0, length);
        return destAry;
    }

    public static CatalogProperties getCatalog(String catalogName, CatalogMate catalogMate)
    {

        List<CatalogProperties> catalogProperties = catalogMate.getCatalogProperties();
        catalogProperties = catalogProperties.stream().filter(catalogPropertie ->
        {
            if (catalogPropertie.getConnectorName().toString().equals(catalogName)) {
                return true;
            }
            return false;
        }).collect(Collectors.toList());
        if (catalogProperties != null && catalogProperties.size() > 0) {
            return catalogProperties.get(0);
        }
        return null;
    }
}




