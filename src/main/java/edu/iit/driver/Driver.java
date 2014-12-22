/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.iit.driver;

import edu.iit.worker.Worker;

/**
 *
 * @author supramo
 */
public class Driver {
    
    public static void main(String[] args){
        Worker worker = new Worker();
        if (worker.checkForMessages()){
            
        }
    }
    
}
