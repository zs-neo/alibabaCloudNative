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

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ClientProcessData implements Runnable {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());
	
	private static List<Map<String, List<String>>> TRACE_CACHE;
	private final ExecutorService exector = Executors.newFixedThreadPool(20);
	
	public ClientProcessData() {
		init();
	}
	
	public static void init() {
		TRACE_CACHE = new ArrayList<>(Constants.CACHE_SIZE);
		for (int i = 0; i < Constants.CACHE_SIZE; i++) {
			TRACE_CACHE.add(new ConcurrentHashMap<>(Constants.BATCH_SIZE));
		}
	}
	
	public static void start() {
		new Thread(new ClientProcessData(), "ClientProcessData").start();
	}
	
	private final static ReentrantLock LOCK = new ReentrantLock();
	private final Condition isEmpty = LOCK.newCondition();
	private final static int RANGE = 1024 * 10;
	private volatile int POS = 0;
	
	@Override
	public void run() {
		String path = "http://118.31.11.163:6868/files/trace1.data";
//		String path = getPath();
		int rangeFrom = 0;
		int rangeTo = RANGE - 1;
		int count = 0;
		Set<java.lang.String> badTraceIds = new HashSet<>(Constants.BADLIST_INIT_SIZE);
		Map<java.lang.String, List<String>> traceMap = TRACE_CACHE.get(POS);
		try {
			Headers headers = new Headers.Builder().add("Range", "bytes=" + rangeFrom + "-" + rangeTo).build();
			Request request = new Request.Builder().url(path).headers(headers).build();
			Response response = Utils.callHttp(request);
			byte[] data = response.body().bytes();
			System.out.println(new String(data));
			// 过滤数据找出问题trace, | = 124 , GET = 71 69 84 , 换行 = 10
			int begin = 0;
			int firstBlockIndex;
			byte[] traceIdBytes = null;
//			printData(data);
			for (int i = 0; i < data.length; i++) {
				// 本行结束，索引 begin-i
				if (data[i] == 10) {
					firstBlockIndex = 0;
					byte[] line = new byte[i - begin];
					for (int k = 0; k < i - begin; k++) {
						line[k] = data[begin + k];
						if (line[k] == 124 && firstBlockIndex == 0) {
							firstBlockIndex = k;
							traceIdBytes = new byte[firstBlockIndex];
							for (int p = 0; p < firstBlockIndex; p++) {
								traceIdBytes[p] = data[begin + p];
							}
						}
					}
					begin = i + 1;
					String lineStr = new String(line);
					String traceId = new String(traceIdBytes);
					if (lineStr.contains("error=1") ||
							((lineStr.contains("http.status_code=") && lineStr.indexOf("http.status_code=200") < 0))) {
						badTraceIds.add(traceId);
						LOGGER.info("add " + traceId);
					}
					List<String> spanList = traceMap.get(traceId);
					if (spanList == null) {
						spanList = new ArrayList<>();
						traceMap.put(traceId, spanList);
					}
					spanList.add(new String(line));
				}
				if (count % Constants.BATCH_SIZE == 0) {
					POS++;
					if (POS >= Constants.CACHE_SIZE) {
						POS = 0;
					}
					traceMap = TRACE_CACHE.get(POS);
					if (traceMap.size() > 0) {
						try {
							LOCK.lock();
							while (traceMap.size() > 0) {
								isEmpty.await();
								LOGGER.warn("client sleeping");
								if (traceMap.size() == 0) {
									isEmpty.signal();
									break;
								}
							}
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							LOCK.unlock();
						}
					}
					int batchPos = count / Constants.BATCH_SIZE - 1;
					updateWrongeTraceId(badTraceIds, batchPos);
					badTraceIds.clear();
				}
			}
			// todo 保存多余的字节
			
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
			return "http://localhost:" + CommonController.getDataSourcePort() + "/trace1.data";
//            return "http://118.31.11.163:6868/files/trace1.data";
//			return "file:///天池大赛/trace1.data";
		} else if ("8001".equals(port)) {
			return "http://localhost:" + CommonController.getDataSourcePort() + "/trace2.data";
//            return "http://118.31.11.163:6868/files/trace2.data";
//			return "file:///天池大赛/trace2.data";
		} else {
			return null;
		}
	}
	
	public static void printData(byte[] data) {
		for (int i = 0; i < data.length; i++) {
			System.out.print(data[i] + " ");
			if (data[i] == 10) {
				System.out.println();
			}
		}
		System.out.println();
		System.out.println(new String(data));
	}
	
	public static void main(String[] args) {
		ClientProcessData.start();
	}
	
}
