package com.alibaba.tailbase;

import com.alibaba.tailbase.backendprocess.BackServer;
import com.alibaba.tailbase.backendprocess.BackServerHandler;
import com.alibaba.tailbase.backendprocess.CheckSumService;
import com.alibaba.tailbase.clientprocess.ClientProcessData;
import com.alibaba.tailbase.clientprocess.ClientTask;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

@EnableAutoConfiguration
@ComponentScan(basePackages = "com.alibaba.tailbase")
public class MultiEntry {
	
	public static void main(String[] args) {
		ClientProcessData.beginTime = System.currentTimeMillis();
		if (Utils.isBackendProcess()) {
			CheckSumService.start();
			BackServerHandler.init();
			BackServer backServer = new BackServer(Integer.parseInt(System.getProperty("server.port")));
			backServer.run();
		}
		if (Utils.isClientProcess()) {
			ClientTask.init();
		}
		String port = System.getProperty("server.port", "8080");
		SpringApplication.run(MultiEntry.class,
				"--server.port=" + port
		);
		
	}
	
	
}
