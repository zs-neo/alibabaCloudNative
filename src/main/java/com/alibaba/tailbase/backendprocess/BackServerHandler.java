/*
 * Author github: https://github.com/zs-neo
 * Author Email: 2931622851@qq.com
 */
package com.alibaba.tailbase.backendprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.Constants;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import lombok.extern.log4j.Log4j2;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.tailbase.Constants.PROCESS_COUNT;
import static io.netty.buffer.Unpooled.copiedBuffer;

/**
 * @author zhousheng
 * @version 1.0
 * @since 2020/6/24 16:15
 */
@Log4j2
public class BackServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
	
	public static volatile Integer FINISH_PROCESS_COUNT = 0;
	
	private static volatile Integer CURRENT_BATCH = 0;
	
	private static int BATCH_COUNT = 200;
	private static List<TraceIdBatch> TRACEID_BATCH_LIST = new ArrayList<>();
	
	public static void init() {
		for (int i = 0; i < BATCH_COUNT; i++) {
			TRACEID_BATCH_LIST.add(new TraceIdBatch());
		}
	}
	
	/**
	 * 处理请求
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext channelHandlerContext, FullHttpRequest fullHttpRequest) {
		String uri = fullHttpRequest.uri();
		FullHttpResponse response = null;
		
		String trueUri = getUri(uri);
//		log.info(trueUri);
		if (fullHttpRequest.method() == HttpMethod.GET) {
			if (trueUri.equals("/ready") || trueUri.equals("/setParameter") || trueUri.equals("/start")) {
				String data = "suc";
				ByteBuf buf = copiedBuffer(data, CharsetUtil.UTF_8);
				response = responseOK(HttpResponseStatus.OK, buf);
			} else if (trueUri.equals("/finish")) {
				FINISH_PROCESS_COUNT++;
				if (FINISH_PROCESS_COUNT >= 2) {
					CheckSumService.sendCheckSum();
					return;
				}
				log.info("receive call 'finish', count:" + FINISH_PROCESS_COUNT);
				ByteBuf buf = copiedBuffer("suc", CharsetUtil.UTF_8);
				response = responseOK(HttpResponseStatus.OK, buf);
			}
		} else if (fullHttpRequest.method() == HttpMethod.POST) {
			if (trueUri.equals("/setWrongTraceId")) {
				String traceIdListJson;
				int batchPos;
				String content = fullHttpRequest.content().toString(Charset.forName("UTF-8"));
				JSONObject data = JSON.parseObject(content);
				traceIdListJson = data.getString("traceIdListJson");
				batchPos = Integer.parseInt(data.getString("batchPos"));
				
				int pos = batchPos % BATCH_COUNT;
				List<String> traceIdList = JSON.parseObject(traceIdListJson, new TypeReference<List<String>>() {
				});
				TraceIdBatch traceIdBatch = TRACEID_BATCH_LIST.get(pos);
				if (traceIdList != null && traceIdList.size() > 0) {
					traceIdBatch.setBatchPos(batchPos);
					traceIdBatch.setProcessCount(traceIdBatch.getProcessCount() + 1);
					traceIdBatch.getTraceIdList().addAll(traceIdList);
				}
				ByteBuf buf = copiedBuffer("suc", CharsetUtil.UTF_8);
				response = responseOK(HttpResponseStatus.OK, buf);
			}
		} else {
			log.warn("ERROR: request type error!");
			response = responseOK(HttpResponseStatus.INTERNAL_SERVER_ERROR, null);
		}
		// 发送响应
		channelHandlerContext.writeAndFlush(response).
				addListener(ChannelFutureListener.CLOSE);
	}
	
	private FullHttpResponse responseOK(HttpResponseStatus status, ByteBuf content) {
		FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
		if (content != null) {
			response.headers().set("Content-Type", "text/plain;charset=UTF-8");
			response.headers().set("Content_Length", response.content().readableBytes());
		}
		return response;
	}
	
	private static String getUri(String uri) {
		char[] charUri = null;
		int i;
		for (i = 0; i < uri.length(); i++) {
			if (uri.charAt(i) == '?') {
				break;
			}
		}
		charUri = new char[i];
		for (int k = 0; k < i; k++) {
			charUri[k] = uri.charAt(k);
		}
		return new String(charUri);
	}
	
	public static boolean isFinished() {
		for (int i = 0; i < BATCH_COUNT; i++) {
			TraceIdBatch currentBatch = TRACEID_BATCH_LIST.get(i);
			if (currentBatch.getProcessCount() != 2) {
				return false;
			}
		}
		if (FINISH_PROCESS_COUNT < Constants.PROCESS_COUNT) {
			return false;
		}
		return true;
	}
	
	/**
	 * get finished bath when current and next batch has all finished
	 *
	 * @return
	 */
	public static TraceIdBatch getFinishedBatch() {
		int next = CURRENT_BATCH + 1;
		if (next >= BATCH_COUNT) {
			next = 0;
		}
		TraceIdBatch nextBatch = TRACEID_BATCH_LIST.get(next);
		TraceIdBatch currentBatch = TRACEID_BATCH_LIST.get(CURRENT_BATCH);
		if ((FINISH_PROCESS_COUNT >= Constants.PROCESS_COUNT) ||
				(nextBatch.getProcessCount() >= PROCESS_COUNT && currentBatch.getProcessCount() >= PROCESS_COUNT)) {
			TraceIdBatch newTraceIdBatch = new TraceIdBatch();
			TRACEID_BATCH_LIST.set(CURRENT_BATCH, newTraceIdBatch);
			CURRENT_BATCH = next;
			return currentBatch;
		}
		
		return null;
	}
}