/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.iit.driver;

import com.amazonaws.services.sqs.model.Message;
import edu.iit.hadoopcluster.HadoopCluster;
import edu.iit.model.User_Jobs;
import edu.iit.worker.Worker;

/**
 *
 * @author supramo
 */
public class Driver {
    
    public static void main(String[] args) throws InterruptedException {
        Worker worker = new Worker();
        while (true) {
            if (worker.checkForMessages()) {
                Message message = worker.getMessages();
                String jobid = message.getBody();
                User_Jobs job = worker.getUserJob(jobid);
                
                Thread t = new Thread(new HadoopCluster(job));
                t.start();
                
                while (t.isAlive()) {
                    Thread.sleep(1000);
                }
                worker.renameAndUploadOutput(job);
                worker.deleteMessage(message, job);
                //worker.sendmail(job);
            } else {
                System.out.println("no Messages we are sleeping");
                Thread.sleep(5000);
            }
        }
        
    }
    
}
