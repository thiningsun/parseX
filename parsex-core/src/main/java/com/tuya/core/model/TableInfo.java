package com.tuya.core.model;

import com.tuya.core.Constants;
import com.tuya.core.enums.OperatorType;
import com.tuya.core.util.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * desc:
 *
 * @author scx
 * @create 2020/02/26
 */
public class TableInfo {

    /**
     * 表名
     */
    private String name;

    /**
     * 库名
     */
    private String dbName;

    private OperatorType type;

    private Set<String> columns;

    private String limit;

    private boolean selectAll;

    private boolean isDb;


    public TableInfo(String dbName, OperatorType type) {
        this.dbName = dbName;
        this.type = type;
        this.isDb = true;
    }

    public TableInfo(String name, String dbName, OperatorType type, HashSet<String> columns) {
        this.name = name;
        this.dbName = dbName;
        this.type = type;
        this.columns = new HashSet<>(columns);
        columns.clear();
        optimizeColumn();
    }

    public TableInfo(String dbAndTableName, OperatorType type, String defaultDb, HashSet<String> columns) {
        if (dbAndTableName.contains(Constants.POINT)) {
            Pair<String, String> pair = StringUtils.getPointPair(dbAndTableName);
            this.name = pair.getRight();
            this.dbName = pair.getLeft();
        } else {
            this.name = dbAndTableName;
            this.dbName = defaultDb;
        }
        this.columns = new HashSet<>(columns);
        this.type = type;
        columns.clear();
        optimizeColumn();
    }


    public Set<String> getColumns() {
        return columns;
    }

    private void optimizeColumn() {
        String dbAndName = this.dbName + Constants.POINT + this.name;
        this.columns = this.columns.stream().map(column -> {
            if (!selectAll && column.endsWith("*")) {
                selectAll = true;
            }
            if (column.contains(Constants.POINT)) {
                Pair<String, String> pair = StringUtils.getLastPointPair(column);
                if (pair.getLeft().equals(dbAndName)) {
                    return pair.getRight();
                }
            }
            return column;
        }).collect(Collectors.toSet());
    }


    public boolean isDb() {
        return isDb;
    }

    public OperatorType getType() {
        return type;
    }

    public String getName() {
        return name;
    }


    public String getDbName() {
        return dbName;
    }

    public String getLimit() {
        return limit;
    }

    public void setLimit(String limit) {
        this.limit = limit;
    }


    public boolean isSelectAll() {
        return selectAll;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (this.columns != null) {
            this.columns.forEach(columns -> builder.append(columns).append(" "));
        }
        if (this.name != null) {
            return (isDb ? "[库]" : "[表]") + dbName + Constants.POINT + name + "[" + type.name() + "] column=[ " + builder.toString() + " ] limit=" + limit + "\n";
        }
        return (isDb ? "[库]" : "[表]") + dbName + "[" + type.name() + "] column=[ " + builder.toString() + " ] limit=" + limit + "\n";

    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TableInfo)) {
            return false;
        }

        TableInfo info = (TableInfo) obj;
        return this.dbName.equals(info.dbName) && this.name.equals(info.name) && this.type == info.type;
    }

    @Override
    public int hashCode() {
        if (this.name != null) {
            return this.dbName.hashCode() + this.name.hashCode() + this.type.hashCode();
        }
        return this.dbName.hashCode() + this.type.hashCode();
    }
}
