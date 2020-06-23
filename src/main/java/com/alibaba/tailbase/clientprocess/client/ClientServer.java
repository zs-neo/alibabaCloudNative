/*
 * Author github: https://github.com/zs-neo
 * Author Email: 2931622851@qq.com
 */
package com.alibaba.tailbase.clientprocess.client;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.stream.ChunkedWriteHandler;
import lombok.extern.log4j.Log4j2;

/**
 * @author zhousheng
 * @version 1.0
 * @since 2020/5/31 20:35
 */
public class ClientServer {
	
	private int inetPort;
	
	public ClientServer(int inetPort) {
		this.inetPort = inetPort;
	}
	
	public void init() throws Exception {
		
		EventLoopGroup parentGroup = new NioEventLoopGroup();
		EventLoopGroup childGroup = new NioEventLoopGroup();
		try {
			ServerBootstrap server = new ServerBootstrap();
			// 1. 绑定两个线程组分别用来处理客户端通道的accept和读写时间
			server.group(parentGroup, childGroup)
					// 2. 绑定服务端通道NioServerSocketChannel
					.channel(NioServerSocketChannel.class)
					// 3. 给读写事件的线程通道绑定handler去真正处理读写
					// ChannelInitializer初始化通道SocketChannel
					.childHandler(new ChannelInitializer<SocketChannel>() {
						@Override
						protected void initChannel(SocketChannel socketChannel) throws Exception {
							// 请求解码器
							socketChannel.pipeline().addLast("http-decoder", new HttpRequestDecoder());
							// 将HTTP消息的多个部分合成一条完整的HTTP消息
							socketChannel.pipeline().addLast("http-aggregator", new HttpObjectAggregator(65535));
							// 响应转码器
							socketChannel.pipeline().addLast("http-encoder", new HttpResponseEncoder());
							// 解决大码流的问题，ChunkedWriteHandler：向客户端发送HTML5文件
							socketChannel.pipeline().addLast("http-chunked", new ChunkedWriteHandler());
							// 自定义处理handler
							socketChannel.pipeline().addLast("http-server", new ClientServerHandler());
						}
					});
			
			// 4. 监听端口（服务器host和port端口），同步返回
			ChannelFuture future = server.bind(this.inetPort).sync();
			// 当通道关闭时继续向后执行，这是一个阻塞方法
			future.channel().closeFuture().sync();
		} finally {
			childGroup.shutdownGracefully();
			parentGroup.shutdownGracefully();
		}
	}
	
	public void run() {
		ClientServer server = new ClientServer(inetPort);
		try {
			server.init();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("exception: " + e.getMessage());
		}
		System.out.println("server close!");
		
	}
	
	public static void main(String[] args) {
		ClientServer server = new ClientServer(8081);
		server.run();
	}
	
}
