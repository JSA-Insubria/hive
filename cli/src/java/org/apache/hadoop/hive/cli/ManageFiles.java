package org.apache.hadoop.hive.cli;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ManageFiles {

    final String path = System.getProperty("user.home")
            + File.separator + "results";

    public ManageFiles() {}

    public void deleteAllFolders() {
        File results = new File(path);
        String[] folders = results.list();
        if (folders != null) {
            for (String folderName : folders) {
                if (folderName.contains("node")) {
                    File folderNode = new File(path + File.separator + folderName);
                    File[] fileInFolder = folderNode.listFiles();
                    if (fileInFolder != null) {
                        deleteAllFiles(fileInFolder);
                    }
                }
            }
        }
    }

    private void deleteAllFiles(File[] fileInFolder) {
        for (File f : fileInFolder) {
            if (f.isDirectory()) {
                File[] file = f.listFiles();
                if (file != null) {
                    deleteAllFiles(file);
                }
            }
            f.delete();
        }
    }

    public void moveFiles() {
        File queryFolder = new File(path + File.separator + "namenode" + File.separator + "QueryDataBlocks");
        if(queryFolder.exists()) {
            move();
        }
    }

    private void move() {
        String sourcePath = path + File.separator + "namenode";
        String dataPath = path + File.separator + "data";
        createDataFolder(dataPath);
        File dataFolder = new File(dataPath);
        String[] queryArray = dataFolder.list();
        if (queryArray != null) {
            checkCase(queryArray, sourcePath, dataPath);
        }
    }

    private void checkCase(String[] queryArray, String sourcePath, String dataPath) {
        if (queryArray.length == 0) { // first query
            String destinationPath = dataPath + File.separator + "q1";
            createfolderTree(dataPath, "q1");
            moveAllFile(sourcePath,destinationPath + File.separator + "q1_1");
        } else { // query already executed
            String queryToMove = getQueryName(sourcePath);
            boolean flag = false;
            for (String qID : queryArray) {
                String queryPathToTest = dataPath + File.separator + qID + File.separator + qID + "_1";
                if (queryToMove.equals(getQueryName(queryPathToTest))) {
                    flag = true;
                    String[] qn_n = new File(dataPath + File.separator + qID).list();
                    if (qn_n != null) {
                        int newEdgeID = Integer.parseInt(qn_n[qn_n.length - 1].split("_")[1]) + 1;
                        moveAllFile(sourcePath, dataPath + File.separator + qID + File.separator + qID + "_" + newEdgeID);
                    }
                }
            }
            if (!flag) { // new query
                int newqID = Integer.parseInt(queryArray[queryArray.length - 1].substring(1)) + 1;
                createfolderTree(dataPath, "q" + Integer.toString(newqID));
                String destinationPath = dataPath + File.separator + "q" + newqID;
                moveAllFile(sourcePath, destinationPath + File.separator + "q" + newqID + "_1");
            }
        }
    }

    private void createfolderTree(String dataPath, String folderRoot) {
        String newPath = dataPath + File.separator + folderRoot;
        File q1 = new File(newPath);
        q1.mkdir();
    }

    private void moveAllFile(String sourcePath, String destPath) {
        if (new File(sourcePath).exists()) {
            try {
                Files.move(Paths.get(sourcePath), Paths.get(destPath));
            } catch (IOException e) {
                e.printStackTrace();
            }
            moveHDFS_read_write(destPath);
        }
    }

    private void moveHDFS_read_write(String destPath) {
        String destinationPath = destPath + File.separator + "hdfs_read_write";
        File dest = new File(destinationPath);
        if (!dest.exists()) {
            dest.mkdir();
        }
        File folder = new File(path);
        String[] folders = folder.list();
        if (folders != null) {
            for (String f : folders) {
                if (!(f.equals("data") || f.equals("namenode"))) {
                    if (f.contains("node")) {
                        String sourceFile = path + File.separator + f + File.separator + "hdfs_read.log";
                        String destinationFile = destinationPath + File.separator + "hdfs_read_" + f + ".log";
                        if (new File(sourceFile).exists()) {
                            try {
                                Files.move(Paths.get(sourceFile), Paths.get(destinationFile));
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        }
    }

    private void createDataFolder(String dataPath) {
        File folder = new File(dataPath);
        if (!folder.exists()) {
            folder.mkdir();
        }
    }

    private String getQueryName(String queryPath) {
        String query = "";
        File queryFolder = new File(queryPath + File.separator + "QueryDataBlocks");
        File[] folderFiles = queryFolder.listFiles();
        if (folderFiles != null) {
            query = getQuery(folderFiles[0]);
        }
        return query;
    }

    private String getQuery(File file) {
        JsonParser jsonParser = new JsonParser();
        Object object = null;
        try {
            object = jsonParser.parse(new FileReader(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        JsonObject jsonObject = (JsonObject) object;
        return jsonObject.get("query").getAsString();
    }
}
