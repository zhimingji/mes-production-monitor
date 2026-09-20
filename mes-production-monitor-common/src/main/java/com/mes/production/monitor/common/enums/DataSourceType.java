package com.mes.production.monitor.common.enums;

import com.mes.production.monitor.common.constant.MesConstants;

/**
 * 数据来源：旧 MES / 新 MES。
 * <p>由 CDC 元数据列 {@code database_name} 的前缀解析得到，见文档 2.1、6.1。
 */
public enum DataSourceType {

    OLD_MES,
    NEW_MES;

    /**
     * 从库名解析数据源。
     *
     * @param databaseName 如 {@code mes_old_zh}
     * @return 解析不出时返回 null（调用方应计入 dirty_record_count）
     */
    public static DataSourceType fromDatabaseName(String databaseName) {
        if (databaseName == null) {
            return null;
        }
        if (databaseName.startsWith(MesConstants.DB_PREFIX_OLD)) {
            return OLD_MES;
        }
        if (databaseName.startsWith(MesConstants.DB_PREFIX_NEW)) {
            return NEW_MES;
        }
        return null;
    }

    /**
     * 从库名解析基地标识。
     *
     * @param databaseName 如 {@code mes_old_zh}
     * @return 如 {@code zh}；解析不出时返回 null
     */
    public static String companyFromDatabaseName(String databaseName) {
        if (databaseName == null) {
            return null;
        }
        if (databaseName.startsWith(MesConstants.DB_PREFIX_OLD)) {
            return databaseName.substring(MesConstants.DB_PREFIX_OLD.length());
        }
        if (databaseName.startsWith(MesConstants.DB_PREFIX_NEW)) {
            return databaseName.substring(MesConstants.DB_PREFIX_NEW.length());
        }
        return null;
    }
}
