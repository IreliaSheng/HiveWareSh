package com.sbx.sh;

import com.sbx.utils.Common;

import java.util.List;
import java.util.Map;

public class HdfsToOds {
    /**
     * load data inpath '/origin_data/${DB}/dmzk0101' OVERWRITE into table ${DB}.ods_dmzk0101 partition(dt='$do_date');
     *
     * @param dataBaseName
     * @return
     */
    public static String splicingHdfsToHive(String dataBaseName) {
        List<Map<String, Object>> tableNameList = Common.selectTableName(dataBaseName);
        StringBuilder sb = new StringBuilder();
        sb.append("sql=\"\n");
        for (Map<String, Object> stringObjectMap : tableNameList) {
            String tableName = (String) stringObjectMap.get("TABLE_NAME");
            String str = "    load data inpath \'" + "/origin_data/${DB}/" + tableName + "\' OVERWRITE into table ${DB}.ods_" + tableName + " partition(dt=\'$do_date\');\n";
            sb.append(str);
        }
        sb.append("\"");
        return sb.toString();
    }
}
