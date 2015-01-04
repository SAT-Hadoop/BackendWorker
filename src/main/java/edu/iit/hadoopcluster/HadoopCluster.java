/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.iit.hadoopcluster;

import edu.iit.model.User_Jobs;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author supramo
 */
public class HadoopCluster implements Runnable{
    
    User_Jobs job;
    
    public HadoopCluster(User_Jobs job){
        this.job = job;
    }
    
    public void getInputFile(){
        try {
            String inputurl = this.job.getInputurl();
            Runtime r = Runtime.getRuntime();
            String url = "https://itmd544.s3.amazonaws.com/" + inputurl;
            r.exec("/usr/bin/wget -o /tmp/inputfile  "+url).waitFor();       
        } catch (IOException|InterruptedException ex) {
            Logger.getLogger(HadoopCluster.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void run() {
        Runtime r = Runtime.getRuntime();
        
        String bin = "/host/DownloadsUbuntu/hadoop-1.2.1/bin/";
        String sbin = "/host/DownloadsUbuntu/hadoop-1.2.1/bin/";
        String jarlocation = "/home/supramo/NetBeansProjects/wordcount/target/wordcount-1.0-SNAPSHOT.jar";
        String mainclass = "edu.iit.wordcount.WordCount";
        try {
            r.exec(bin + "stop-all.sh").waitFor();
            r.exec(bin + "hadoop namenode -format -force").waitFor();
            r.exec(bin + "start-all.sh").waitFor();
            r.exec(bin + "hadoop fs -rm /input/inputfile").waitFor();
            r.exec(bin + "hadoop fs -rmr /output").waitFor();
            r.exec(bin + "hadoop fs -mkdir /input").waitFor();
            r.exec(bin + "hadoop fs -put /tmp/inputfile /input/").waitFor();
            r.exec(bin + "hadoop jar " + jarlocation +" " +mainclass +" /input/inputfile /output").waitFor();    
        } catch (Exception ex) {
            System.out.println("something messed up");
        }
    }
    
}
