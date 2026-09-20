package com.mes.production.monitor.common.model;

import java.io.Serializable;

/**
 * 维表 POJO 集合。数据量小（&lt; 万级），用 Broadcast State + open() 里 JDBC bootstrap 兜底，
 * 见文档 6.2 / 6.2.1。
 *
 * <p>放在一个文件里是刻意的：这些类都只是数据容器，分散成 5 个文件反而不利于对照 schema。
 */
public final class DimTables {

    private DimTables() {
    }

    /** 生产订单。既是维表也是计划量来源，是 DWD 订单关联的关键 */
    public static class ProductionOrder implements Serializable {
        private static final long serialVersionUID = 1L;

        public long productionOrderId;
        /** 订单编号，DWD 统一后的键 */
        public String productionOrderNo;
        public long orderQuantity;
        public long worksiteId;
        public long partId;
        /** 创建时间 epoch millis，计划爬坡的起点，见文档 7.3 */
        public long createdDate;
        /** 交期 epoch millis，计划爬坡的终点 + 滞后预警依据 */
        public long dueDate;

        public ProductionOrder() {
        }
    }

    public static class Part implements Serializable {
        private static final long serialVersionUID = 1L;

        public long partId;
        public String partCode;
        public String partName;

        public Part() {
        }
    }

    /** 工作中心 = 产线。如 workCenterName=D32Z01, description=总装A线, workCenterType=总装 */
    public static class WorkCenter implements Serializable {
        private static final long serialVersionUID = 1L;

        public long workCenterId;
        public String workCenterName;
        public String description;
        public long worksiteId;
        public String workCenterType;

        public WorkCenter() {
        }
    }

    /** 车间/分厂。如 涡旋分厂 / 总装分厂 / 泵体分厂 */
    public static class WorkSite implements Serializable {
        private static final long serialVersionUID = 1L;

        public long worksiteId;
        public String description;

        public WorkSite() {
        }
    }

    public static class User implements Serializable {
        private static final long serialVersionUID = 1L;

        public long userId;
        public String userName;

        public User() {
        }
    }
}
