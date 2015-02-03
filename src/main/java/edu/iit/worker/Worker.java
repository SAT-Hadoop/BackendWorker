/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.iit.worker;

import edu.iit.doa.DOA;
import edu.iit.model.User_Jobs;
import edu.iit.sqs.SendQueue;
import java.io.IOException;
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
    String queuename;
    public Worker(){
        String ipaddress;
        try {
            ipaddress = Inet4Address.getLocalHost().getHostAddress();
            
            DOA doa = new DOA();
            this.queuename = doa.getEc2Queue(ipaddress);
            System.out.println(ipaddress+":"+this.queuename);
        } catch (UnknownHostException ex) {
            Logger.getLogger(Worker.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
        
    }
    public boolean checkForMessages(){
        
        //this.queuename = "https://sqs.us-east-1.amazonaws.com/961412573847/sai4";
        return sendq.checkForMessages(this.queuename);
    }
    
    
    public void getInputFile(String filelink) {
        try {
            Runtime r = Runtime.getRuntime();
            r.exec("/usr/bin/wget -o /tmp/inputfile  "+filelink).waitFor();
        } catch (IOException|InterruptedException ex) {
            Logger.getLogger(Worker.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public String getMessages(){
        return sendq.getMessage().getBody();
    }
    
    public User_Jobs getUserJob(String jobid){
        return doa.getUserJob(jobid);
    }
}

