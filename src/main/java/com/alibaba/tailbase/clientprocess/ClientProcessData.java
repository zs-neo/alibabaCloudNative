package com.alibaba.tailbase.clientprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.CommonController;
import com.alibaba.tailbase.Constants;
import com.alibaba.tailbase.Utils;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


public class ClientProcessData implements Runnable {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());
	
	private static List<Map<String, List<String>>> TRACE_CACHE;
	
	public ClientProcessData() {
		init();
	}
	
	public static void start() {
		new Thread(new ClientProcessData(), "ClientProcessData").start();
	}
	
	public static void init() {
		TRACE_CACHE = new ArrayList<>(Constants.CACHE_SIZE);
		for (int i = 0; i < Constants.CACHE_SIZE; i++) {
			TRACE_CACHE.add(new ConcurrentHashMap<>(Constants.BATCH_SIZE));
		}
	}
	
	@Override
	public void run() {
		String path = getPath();
		try {
			URL url = new URL(path);
			LOGGER.info("data path:" + path);
//			HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
//			InputStream input = httpConnection.getInputStream();
//			BufferedReader bf = new BufferedReader(new InputStreamReader(input));
			InputStream input = url.openStream();
			BufferedReader bf = new BufferedReader(new InputStreamReader(input));
			String line;
			int count = 0;
			int pos = 0;
			Set<String> badTraceIds = new HashSet<>(Constants.BADLIST_INIT_SIZE);
			Map<String, List<String>> traceMap = TRACE_CACHE.get(pos);
			while ((line = bf.readLine()) != null) {
				count++;
				String[] rowItem = line.split("\\|");
				String traceId = rowItem[0];
				String status = rowItem[8];
				List<String> spanList = traceMap.get(traceId);
				if (spanList == null) {
					spanList = new ArrayList<>();
					traceMap.put(traceId, spanList);
				}
				spanList.add(line);
				if (status.contains("error=1")) {
					badTraceIds.add(traceId);
				} else if (status.contains("http.status_code=") && status.indexOf("http.status_code=200") < 0) {
					badTraceIds.add(traceId);
				}
				if (count % Constants.BATCH_SIZE == 0) {
					pos++;
					if (pos >= Constants.CACHE_SIZE) {
						pos = 0;
					}
					traceMap = TRACE_CACHE.get(pos);
					if (traceMap.size() > 0) {
						while (true) {
							try {
								Thread.sleep(10);
								LOGGER.warn("client sleeping");
								if (traceMap.size() == 0) {
									break;
								}
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
					int batchPos = count / Constants.BATCH_SIZE - 1;
					updateWrongeTraceId(badTraceIds, batchPos);
					badTraceIds.clear();
				}
			}
			updateWrongeTraceId(badTraceIds, count / Constants.BATCH_SIZE - 1);
			bf.close();
			input.close();
			callFinish();
		} catch (IOException e) {
			LOGGER.warn("拉数据时异常");
		}
	}
	
	public void updateWrongeTraceId(Set<String> badTraceIds, int batchPos) {
		if (badTraceIds.size() == 0) {
			return;
		}
		String json = JSON.toJSONString(badTraceIds);
		if (badTraceIds.size() > 0) {
			try {
				LOGGER.info("updateBadTraceId, json:" + json + ", batch:" + batchPos);
				JSONObject data = new JSONObject();
				data.put("traceIdListJson", json);
				data.put("batchPos", batchPos);
				RequestBody body = RequestBody.create(MediaType.parse("application/json"), JSONObject.toJSONString(data));
				Request request = new Request.Builder().url("http://localhost:8002/setWrongTraceId").post(body).build();
				Response response = Utils.callHttp(request);
				response.close();
			} catch (Exception e) {
				LOGGER.warn("向后端更新异常链路失败 " + json + " " + batchPos);
			}
		}
	}
	
	public void callFinish() {
		try {
			Request request = new Request.Builder().url("http://localhost:8002/finish").build();
			Response response = Utils.callHttp(request);
			response.close();
		} catch (Exception e) {
			LOGGER.warn("请求finish异常");
		}
	}
	
	
	public static String getWrongeTracing(String wrongTraceIdListJson, int batchPos) {
		List<String> wrongTraceIdList = JSON.parseObject(wrongTraceIdListJson, new TypeReference<List<String>>() {
		});
		Map<String, List<String>> wrongeTraceMap = new HashMap<>();
		int pos = batchPos % Constants.CACHE_SIZE;
		int previous = pos - 1;
		if (previous < 0) {
			previous = Constants.CACHE_SIZE - 1;
		}
		int next = pos + 1;
		if (next == Constants.CACHE_SIZE) {
			next = 0;
		}
		getWrongTraceWithBatch(previous, pos, wrongTraceIdList, wrongeTraceMap);
		getWrongTraceWithBatch(pos, pos, wrongTraceIdList, wrongeTraceMap);
		getWrongTraceWithBatch(next, pos, wrongTraceIdList, wrongeTraceMap);
		TRACE_CACHE.get(previous).clear();
		LOGGER.info("clear batch " + previous);
		return JSON.toJSONString(wrongeTraceMap);
	}
	
	public static void getWrongTraceWithBatch(int batchPos, int pos, List<String> wrongTraceIdList, Map<String, List<String>> wrongTraceMap) {
		Map<String, List<String>> traceMap = TRACE_CACHE.get(batchPos);
		for (int i = 0; i < wrongTraceIdList.size(); i++) {
			String wrongeTraceId = wrongTraceIdList.get(i);
			List<String> spans = traceMap.get(wrongeTraceId);
			if (spans != null) {
				List<String> existSpanList = wrongTraceMap.get(wrongeTraceId);
				if (existSpanList != null) {
					existSpanList.addAll(spans);
				} else {
					wrongTraceMap.put(wrongeTraceId, spans);
				}
			}
		}
	}
	
	private String getPath() {
		String port = System.getProperty("server.port", "8080");
		if ("8000".equals(port)) {
//            return "http://localhost:" + CommonController.getDataSourcePort() + "/trace1.data";
//            return "http://118.31.11.163:6868/files/trace1.data";
			return "file:///天池大赛/trace1.data";
		} else if ("8001".equals(port)) {
//            return "http://localhost:" + CommonController.getDataSourcePort() + "/trace2.data";
//            return "http://118.31.11.163:6868/files/trace2.data";
			return "file:///天池大赛/trace2.data";
		} else {
			return null;
		}
	}
	
}
