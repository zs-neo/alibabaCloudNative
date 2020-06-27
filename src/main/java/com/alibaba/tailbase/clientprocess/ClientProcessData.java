package com.alibaba.tailbase.clientprocess;

import com.alibaba.tailbase.CommonController;
import com.alibaba.tailbase.Utils;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

import static com.alibaba.tailbase.clientprocess.ClientTask.RANGE;

public class ClientProcessData implements Runnable {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());
	
	private final static ExecutorService exector = Executors.newFixedThreadPool(3);
	public static long beginTime;
	
	public ClientProcessData() {
		LOGGER.warn("e=begin at " + beginTime);
	}
	
	@Override
	public void run() {
		Request request = new Request.Builder().url(getPath()).head().build();
		try {
			Response response = Utils.callHttp(request);
			long size = Long.parseLong(response.headers().get("Content-Length"));
			long batchCount = size / RANGE;
			for (int i = 0; i < batchCount + 1; i++) {
				Callable task = new ClientTask();
				exector.submit(task);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private String getPath() {
		String port = System.getProperty("server.port", "8080");
		if ("8000".equals(port)) {
			return "http://localhost:" + CommonController.getDataSourcePort() + "/trace1.data";
//			return "http://118.31.11.163:6868/files/trace1.data";
//			return "file:///天池大赛/trace1.data";
		} else if ("8001".equals(port)) {
			return "http://localhost:" + CommonController.getDataSourcePort() + "/trace2.data";
//			return "http://118.31.11.163:6868/files/trace2.data";
//			return "file:///天池大赛/trace2.data";
		} else {
			return null;
		}
	}
	
}
