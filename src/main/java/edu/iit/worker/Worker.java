/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.iit.worker;

import edu.iit.doa.DOA;
import edu.iit.model.User_Jobs;
import edu.iit.sqs.SendQueue;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author supramo
 */
public class Worker{
    SendQueue sendq = new SendQueue();
    DOA doa = new DOA();
    String queuename = "https://sqs.us-east-1.amazonaws.com/961412573847/sai4";
    public Worker(){
        String ipaddress;
        try {
            ipaddress = Inet4Address.getLocalHost().getHostAddress();
            DOA doa = new DOA();
            this.queuename = doa.getEc2Queue(ipaddress);
        } catch (UnknownHostException ex) {
            Logger.getLogger(Worker.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
        
    }
    public boolean checkForMessages(){
        
        //this.queuename = "https://sqs.us-east-1.amazonaws.com/961412573847/sai4";
        return sendq.checkForMessages(this.queuename);
    }
    
    public String getMessages(){
        return sendq.getMessage(queuename).getBody();
    }
    
    public User_Jobs getUserJob(String jobid){
        return doa.getUserJob(jobid);
    }
}

