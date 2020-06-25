package org.apache.hadoop.hive.ql;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryDataInfo {

    private final QueryPlan queryPlan;

    private final ObjectMapper objectMapper;
    private final QueryData queryData;

    private final Map<String, MapRedStats> stats;

    public QueryDataInfo(QueryPlan queryPlan, Map<String, MapRedStats> stats) {
        this.queryPlan = queryPlan;
        objectMapper = new ObjectMapper();
        queryData = new QueryData();
        this.stats = stats;
    }

    public void getDataAfterCompile() {
        String query = queryPlan.getQueryStr();
        if(query.toLowerCase().startsWith("select") || query.toLowerCase().startsWith("explain select")) {
            queryData.setQuery(queryPlan.getQueryStr());
            //queryData.setQueryPlan(queryPlan.toString());
            getTableInfo();
            saveJsonFile();
            printTime(queryPlan.getQuery().getQueryId());
            printCPUTime(getCPUTimeString(queryPlan.getQuery().getQueryId()));
        }
    }

    private void printTime(String line) {
        File fileName = new File(System.getProperty("user.home") + File.separator + "QueryExecutionTime.log");
        try {
            FileWriter myWriter = new FileWriter(fileName, true);
            myWriter.write(line + ": ");
            myWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getCPUTimeString(String queryID) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(queryID).append("\n");
        long totalCpu = 0;
        for (Map.Entry<String, MapRedStats> entry : stats.entrySet()) {
            stringBuilder.append("Stage-").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            totalCpu += entry.getValue().getCpuMSec();
        }
        stringBuilder.append("Total MapReduce CPU Time Spent: ").append(Utilities.formatMsecToStr(totalCpu)).append("\n");
        return stringBuilder.toString();
    }

    private void printCPUTime(String line) {
        File fileName = new File(System.getProperty("user.home") + File.separator + "QueryCPUTime.log");
        try {
            FileWriter myWriter = new FileWriter(fileName, true);
            myWriter.write(line + "\n");
            myWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void getTableInfo() {
        List<QueryDataTable> queryDataTableList = new ArrayList<>();
        Map<String, Table> map = getTablesFromPlan(queryPlan);
        Path path = null;
        for (Map.Entry<String, Table> mp : map.entrySet()) {
            path = mp.getValue().getPath();
            if(path != null) {
                try {
                    queryDataTableList.add(new QueryDataTable(mp.getKey(), getFileInfo(FileContext.getFileContext(), path)));
                } catch (UnsupportedFileSystemException e) {
                    e.printStackTrace();
                }
            }
        }
        queryData.setTableList(queryDataTableList);
    }

    private List<QueryDataFile> getFileInfo(FileContext fileContext, Path path) {
        List<QueryDataFile> queryDataFileList = new ArrayList<>();
        try {
            RemoteIterator<LocatedFileStatus> remoteIterator = fileContext.listLocatedStatus(path);
            while (remoteIterator.hasNext()) {
                LocatedFileStatus locatedFileStatus = remoteIterator.next();
                if(locatedFileStatus.isFile()) {
                    queryDataFileList.add(new QueryDataFile(locatedFileStatus.toString(), getBlockInfo(locatedFileStatus.getBlockLocations())));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return queryDataFileList;
    }

    private List<QueryDataBlock> getBlockInfo(BlockLocation[] blockLocations) {
        List<QueryDataBlock> queryDataBlockList = new ArrayList<>();
        try {
            int blockIndex = 0;
            for (BlockLocation blockLocation : blockLocations) {
                List<QueryDataReplica> queryDataReplicaList = new ArrayList<>();
                for (int i = 0; i < blockLocation.getNames().length; i++) {
                    queryDataReplicaList.add(new QueryDataReplica((i+1),
                            blockLocation.getNames()[i],
                            blockLocation.getStorageIds()[i],
                            blockLocation.getStorageTypes()[i].name(),
                            blockLocation.getTopologyPaths()[i]));
                }
                queryDataBlockList.add(new QueryDataBlock(blockIndex++, queryDataReplicaList));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return queryDataBlockList;
    }

    private Map<String, Table> getTablesFromPlan(QueryPlan plan) {
        Map<String, Table> tables = new HashMap<>();
        plan.getInputs().forEach(
                input -> addTableFromEntity(input, tables)
        );
        return tables;
    }

    private void addTableFromEntity(Entity entity, Map<String, Table> tables) {
        Table tbl;
        switch (entity.getType()) {
            case TABLE: {
                tbl = entity.getTable();
                break;
            }
            case PARTITION:
            case DUMMYPARTITION: {
                tbl = entity.getPartition().getTable();
                break;
            }
            default: {
                return;
            }
        }
        String fullTableName = AcidUtils.getFullTableName(tbl.getDbName(), tbl.getTableName());
        tables.put(fullTableName, tbl);
    }

    private void saveJsonFile() {
        File jsonDirectory = new File(System.getProperty("user.home") + File.separator + "QueryDataBlocks");
        if (!jsonDirectory.exists()) {
            jsonDirectory.mkdir();
        }
        if (jsonDirectory.exists()) {
            try {
                objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(jsonDirectory + File.separator + queryPlan.getQuery().getQueryId() + ".json"), queryData);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private class QueryData {
        private String query;
        private List<QueryDataTable> tableList;

        public QueryData() {}

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public List<QueryDataTable> getTableList() {
            return tableList;
        }

        public void setTableList(List<QueryDataTable> tableList) {
            this.tableList = tableList;
        }
    }

    private class QueryDataTable {
        private String tableName;
        private List<QueryDataFile> queryDataFileList;

        public QueryDataTable(String tableName, List<QueryDataFile> queryDataFileList) {
            this.tableName = tableName;
            this.queryDataFileList = queryDataFileList;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public List<QueryDataFile> getQueryDataFileList() {
            return queryDataFileList;
        }

        public void setQueryDataFileList(List<QueryDataFile> queryDataFileList) {
            this.queryDataFileList = queryDataFileList;
        }
    }

    private class QueryDataFile {
        private String file;
        private List<QueryDataBlock> blockList;

        public QueryDataFile(String file, List<QueryDataBlock> blockList) {
            this.file = file;
            this.blockList = blockList;
        }

        public String getFile() {
            return file;
        }

        public void setFile(String file) {
            this.file = file;
        }

        public List<QueryDataBlock> getBlockList() {
            return blockList;
        }

        public void setBlockList(List<QueryDataBlock> blockList) {
            this.blockList = blockList;
        }
    }

    private class QueryDataBlock {
        private int blockId;
        private List<QueryDataReplica> replicaList;

        public QueryDataBlock(int blockId, List<QueryDataReplica> replicaList) {
            this.blockId = blockId;
            this.replicaList = replicaList;
        }

        public int getBlockId() {
            return blockId;
        }

        public void setBlockId(int blockId) {
            this.blockId = blockId;
        }

        public List<QueryDataReplica> getReplicaList() {
            return replicaList;
        }

        public void setReplicaList(List<QueryDataReplica> replicaList) {
            this.replicaList = replicaList;
        }
    }

    private class QueryDataReplica {
        private int replicaId;
        private String location;
        private String storageId;
        private String storageType;
        private String topologyPath;

        public QueryDataReplica(int replicaId, String location, String storageId, String storageType, String topologyPath) {
            this.replicaId = replicaId;
            this.location = location;
            this.storageId = storageId;
            this.storageType = storageType;
            this.topologyPath = topologyPath;
        }

        public int getReplicaId() {
            return replicaId;
        }

        public void setReplicaId(int replicaId) {
            this.replicaId = replicaId;
        }

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }

        public String getStorageId() {
            return storageId;
        }

        public void setStorageId(String storageId) {
            this.storageId = storageId;
        }

        public String getStorageType() {
            return storageType;
        }

        public void setStorageType(String storageType) {
            this.storageType = storageType;
        }

        public String getTopologyPath() {
            return topologyPath;
        }

        public void setTopologyPath(String topologyPath) {
            this.topologyPath = topologyPath;
        }
    }

}
