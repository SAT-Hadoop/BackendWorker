/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.iit.hadoopcluster;

import edu.iit.model.User_Jobs;
import java.io.File;
import java.io.IOException;
import java.util.Map;
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

    @Override
    public void run() {
        Map<String, String> env = System.getenv();
        String home = env.get("HOME");
        System.out.println(home);
        File f = new File("/tmp/inputfile");
        if (!f.exists()){
            System.out.println("No such file buddy");
            System.exit(1);
        }
        Runtime r = Runtime.getRuntime();
        String bin = home + "/hadoop-2.6.0/bin/";
        String sbin = home + "/hadoop-2.6.0/sbin/";
        String jarlocation = home + "/wordcount-1.0-SNAPSHOT.jar";
        String mainclass = "edu.iit.wordcount.WordCount";
        try {
            r.exec(sbin + "stop-all.sh").waitFor();
            r.exec(bin + "hadoop namenode -format -force").waitFor();
            r.exec(sbin + "start-all.sh").waitFor();
            System.out.println("starting up the nodes");
            r.exec(bin + "hadoop fs -rm /input/inputfile").waitFor();
            r.exec(bin + "hadoop fs -rmr /output").waitFor();
            r.exec(bin + "hadoop fs -mkdir /input").waitFor();
            r.exec(bin + "hadoop fs -put /tmp/inputfile /input/").waitFor();
            r.exec(bin + "hadoop jar " + jarlocation +" " +mainclass +" /input/inputfile /output").waitFor();    
            r.exec(bin + "hadoop fs -get /output /tmp/").waitFor();            
            r.exec("gzip /tmp/output").waitFor();
            r.exec(sbin + "stop-all.sh");
            
        } catch (Exception ex) {
            Logger.getLogger(HadoopCluster.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
