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
            r.exec("wget -o /tmp/  "+url).waitFor();
            r.exec("hadoop namenode -format").waitFor();
            r.exec("hadoop -fs mkdir /usr").waitFor();
            r.exec("hadoop -fs mkdir /usr/input").waitFor();
            r.exec("hadoop -fs put /tmp/"+inputurl+" /usr/input/").waitFor();
            
            
        } catch (IOException|InterruptedException ex) {
            Logger.getLogger(HadoopCluster.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void run() {
        
         
        try {
            Runtime r = Runtime.getRuntime();
            r.exec("hadoop jar worcount /usr/input /usr/output").waitFor();
        } catch (IOException|InterruptedException ex) {
            Logger.getLogger(HadoopCluster.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
