package com.ibm.o2.billing.dashboard.refresh;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
 
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
 
public class RefreshFiles implements ServletContextListener {
 
    private ScheduledExecutorService scheduler;
    private Runnable command;
 
    @Override
    public void contextInitialized(ServletContextEvent event) {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        command = new RefreshThread(event.getServletContext());
        // Delay 0 Minutes to first execution
        long initialDelay = 1;
        // period the period between successive executions
        long period = 30;// 10 Seconds
 
        scheduler.scheduleAtFixedRate(command, initialDelay, period, TimeUnit.SECONDS);
    }
 
    @Override
    public void contextDestroyed(ServletContextEvent event) {
    	((RefreshThread) command).shutdown();
        scheduler.shutdownNow();
    }
 
}