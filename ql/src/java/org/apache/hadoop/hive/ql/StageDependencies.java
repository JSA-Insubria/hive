package org.apache.hadoop.hive.ql;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class StageDependencies {

    private String explainOutput;
    private String queryId;

    public StageDependencies(String queryId, String explainOutput) {
        this.queryId = queryId;
        this.explainOutput = explainOutput;
    }

    public void printDependencies() {
        /*
        String dependencies = "";
        String[] split = explainOutput.split("\n");
        for (String tmp : split) {
            if (tmp.contains("STAGE PLANS")) {
                break;
            } else {
                dependencies = dependencies + "\n" + tmp;
            }
        }
        printIntoJobSubmitterLog(queryId, dependencies);
        */
        printIntoJobSubmitterLog(queryId, explainOutput);
    }

    private void printIntoJobSubmitterLog(String queryId, String line) {
        File fileDir = new File(System.getProperty("user.home")
                + File.separator + "results"
                + File.separator + "namenode");
        if (!fileDir.exists()) {
            fileDir.mkdir();
        }
        File jsonDirectory = new File(fileDir + File.separator + "ExplainQuery");
        if (!jsonDirectory.exists()) {
            jsonDirectory.mkdir();
        }
        if (jsonDirectory.exists()) {
            File fileName = new File(jsonDirectory + File.separator + queryId + ".log");
            try {
                FileWriter myWriter = new FileWriter(fileName, true);
                myWriter.write(line + "\n");
                myWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
