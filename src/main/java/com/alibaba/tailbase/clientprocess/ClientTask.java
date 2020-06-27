/*
 * Author github: https://github.com/zs-neo
 * Author Email: 2931622851@qq.com
 */
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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author zhousheng
 * @version 1.0
 * @since 2020/6/25 9:37
 */
public class ClientTask implements Callable {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());
	
	public final static int RANGE = 1024 * 1024 * 5;
	public volatile static int rangeFrom = RANGE * (-1);
	
	/**
	 * ring buffer
	 * 一行数据最大300字节，那么2核4g的内存按3.5g算可以保存12526000+的行
	 * 也就是600+个20000行
	 * 同理1核2g按1.5g算可以保存200+个20000行
	 */
	private static List<Map<String, List<String>>> TRACE_CACHE;
	private static Map<Integer, String> FIRST_ROW;
	private static Map<Integer, String> LAST_ROW;
	
	public ClientTask() {
	}
	
	public static void init() {
		TRACE_CACHE = new ArrayList<>(Constants.CACHE_SIZE);
		FIRST_ROW = new HashMap<>(Constants.CACHE_SIZE);
		LAST_ROW = new HashMap<>(Constants.CACHE_SIZE);
		for (int i = 0; i < Constants.CACHE_SIZE; i++) {
			TRACE_CACHE.add(new ConcurrentHashMap<>(Constants.BATCH_SIZE));
		}
	}
	
	@Override
	public Object call() throws Exception {
		if (System.currentTimeMillis() - ClientProcessData.beginTime > 10000) {
			LOGGER.warn("e=end at " + System.currentTimeMillis());
			callFinish();
			return -1;
		}
		rangeFrom += RANGE;
		int pos = (rangeFrom / RANGE) % Constants.CACHE_SIZE;
		String rangeStr = "bytes=" + rangeFrom + "-" + (rangeFrom + RANGE);
		LOGGER.info("run Thread: " + rangeStr);
		try {
			Map<String, List<String>> traceMap = TRACE_CACHE.get(pos);
			Set<String> badTraceIds = new HashSet<>(Constants.BADLIST_INIT_SIZE);
			Headers headers = new Headers.Builder().add("Range", rangeStr).build();
//			String path = "http://118.31.11.163:6868/files/trace1.data";
			String path = getPath();
			Request request = new Request.Builder().url(path).headers(headers).build();
			Response response = Utils.callHttp(request);
			byte[] data = response.body().bytes();
			if (data.length < RANGE) {
				// 说明是最后一行
				callFinish();
				return -1;
			}
//			System.out.println(new String(data));
			// 过滤数据找出问题trace, | = 124 , GET = 71 69 84 , 换行 = 10
			int begin = 0;
			int firstBlockIndex;
			byte[] traceIdBytes = null;
			for (int i = 0; i < data.length; i++) {
				// 本行结束，索引 begin-i
				if (data[i] == 10 && begin == 0) {
					// first row
					byte[] line = new byte[i - begin];
					for (int k = 0; k < i; k++) {
						line[k] = data[k];
					}
					begin = i + 1;
					FIRST_ROW.put(pos, new String(line));
				} else if (i == data.length - 1 && begin < i) {
					// last row
					byte[] line = new byte[i - begin];
					for (int k = 0; k < i - begin; k++) {
						line[k] = data[begin + k];
					}
					begin = i + 1;
					LAST_ROW.put(pos, new String(line));
				} else if (data[i] == 10) {
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
					// 找到当前行的status是否异常
					if (lineStr.contains("error=1") ||
							((lineStr.contains("http.status_code=") && lineStr.indexOf("http.status_code=200") < 0))) {
						badTraceIds.add(traceId);
						LOGGER.info("add " + traceId);
					}
					// 缓存tracId的span列表
					List<String> spanList = traceMap.get(traceId);
					if (spanList == null) {
						spanList = new ArrayList<>();
						traceMap.put(traceId, spanList);
					}
					spanList.add(lineStr);
				}
			}
			// todo 环形缓存处理
			updateWrongeTraceId(badTraceIds, pos);
			badTraceIds.clear();
			// todo 处理多余的字节
			
		} catch (
				Exception e) {
			e.printStackTrace();
		}
		return null;
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
			LOGGER.info("call finish");
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
