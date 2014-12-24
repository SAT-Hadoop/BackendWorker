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
        String inputurl = this.job.getInputurl();
        
    }

    @Override
    public void run() {
        
         
        try {
            Process p = Runtime.getRuntime().exec("echo 'sa'");
            p.waitFor();
        } catch (IOException|InterruptedException ex) {
            Logger.getLogger(HadoopCluster.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
