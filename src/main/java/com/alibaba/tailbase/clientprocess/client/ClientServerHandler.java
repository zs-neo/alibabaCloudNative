/*
 * Author github: https://github.com/zs-neo
 * Author Email: 2931622851@qq.com
 */
package com.alibaba.tailbase.clientprocess.client;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.tailbase.clientprocess.ClientProcessData;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.MemoryAttribute;
import io.netty.util.CharsetUtil;
import lombok.extern.log4j.Log4j2;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.netty.buffer.Unpooled.copiedBuffer;

/**
 * @author zhousheng
 * @version 1.0
 * @since 2020/5/31 20:35
 */
@Log4j2
public class ClientServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
	
	/**
	 * 处理请求
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext channelHandlerContext, FullHttpRequest fullHttpRequest) {
		log.info(fullHttpRequest);
		
		String uri = fullHttpRequest.uri();
		FullHttpResponse response = null;
		
		String trueUri = getUri(uri);
		log.info(trueUri);
		
		if (fullHttpRequest.method() == HttpMethod.GET) {
			if (trueUri.equals("/getWrongTrace")) {
				QueryStringDecoder decoder = new QueryStringDecoder(fullHttpRequest.getUri());
				Map<String, List<String>> parame = decoder.parameters();
				Iterator<Map.Entry<String, List<String>>> iterator = parame.entrySet().iterator();
				String traceIdList = null;
				String batchPos = null;
				while (iterator.hasNext()) {
					Map.Entry<String, List<String>> next = iterator.next();
					traceIdList = next.getValue().get(0);
					batchPos = next.getValue().get(0);
				}
				log.info("PARAM: " + traceIdList + " " + batchPos);
				String json = ClientProcessData.getWrongTracing(traceIdList, Integer.parseInt(batchPos));
//				String data = "GET method over";
				ByteBuf buf = copiedBuffer(json, CharsetUtil.UTF_8);
				response = responseOK(HttpResponseStatus.OK, buf);
			}
		} else {
			log.warn("ERROR: request type error!");
			response = responseOK(HttpResponseStatus.INTERNAL_SERVER_ERROR, null);
		}
		// 发送响应
		channelHandlerContext.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
	}
	
	/**
	 * 获取GET方式传递的参数
	 */
	private Map<String, Object> getGetParamsFromChannel(FullHttpRequest fullHttpRequest) {
		
		Map<String, Object> params = new HashMap<String, Object>();
		
		if (fullHttpRequest.method() == HttpMethod.GET) {
			// 处理get请求
			QueryStringDecoder decoder = new QueryStringDecoder(fullHttpRequest.uri());
			Map<String, List<String>> paramList = decoder.parameters();
			for (Map.Entry<String, List<String>> entry : paramList.entrySet()) {
				params.put(entry.getKey(), entry.getValue().get(0));
			}
			return params;
		} else {
			return null;
		}
		
	}
	
	/**
	 * 获取POST方式传递的参数
	 */
	private Map<String, Object> getPostParamsFromChannel(FullHttpRequest fullHttpRequest) {
		
		Map<String, Object> params = new HashMap<String, Object>();
		
		if (fullHttpRequest.method() == HttpMethod.POST) {
			// 处理POST请求
			String strContentType = fullHttpRequest.headers().get("Content-Type").trim();
			if (strContentType.contains("x-www-form-urlencoded")) {
				params = getFormParams(fullHttpRequest);
			} else if (strContentType.contains("application/json")) {
				try {
					params = getJSONParams(fullHttpRequest);
				} catch (UnsupportedEncodingException e) {
					return null;
				}
			} else {
				return null;
			}
			return params;
		} else {
			return null;
		}
	}
	
	/**
	 * 解析from表单数据（Content-Type = x-www-form-urlencoded）
	 */
	private Map<String, Object> getFormParams(FullHttpRequest fullHttpRequest) {
		Map<String, Object> params = new HashMap<String, Object>();
		
		HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(new DefaultHttpDataFactory(false), fullHttpRequest);
		List<InterfaceHttpData> postData = decoder.getBodyHttpDatas();
		
		for (InterfaceHttpData data : postData) {
			if (data.getHttpDataType() == InterfaceHttpData.HttpDataType.Attribute) {
				MemoryAttribute attribute = (MemoryAttribute) data;
				params.put(attribute.getName(), attribute.getValue());
			}
		}
		
		return params;
	}
	
	/**
	 * 解析json数据（Content-Type = application/json）
	 */
	private Map<String, Object> getJSONParams(FullHttpRequest fullHttpRequest) throws UnsupportedEncodingException {
		Map<String, Object> params = new HashMap<String, Object>();
		
		ByteBuf content = fullHttpRequest.content();
		byte[] reqContent = new byte[content.readableBytes()];
		content.readBytes(reqContent);
		String strContent = new String(reqContent, "UTF-8");
		
		JSONObject jsonParams = JSONObject.parseObject(strContent);
		for (Object key : jsonParams.keySet()) {
			params.put(key.toString(), jsonParams.get(key));
		}
		return params;
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
	
}