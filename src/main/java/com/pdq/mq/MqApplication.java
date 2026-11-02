package com.pdq.mq;

import com.pdq.mq.mqserver.BrokerServer;
import org.springframework.beans.BeansException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;

@SpringBootApplication
public class MqApplication implements ApplicationContextAware {
	public static ConfigurableApplicationContext context;

	public static void main(String[] args) throws IOException {
		context = SpringApplication.run(MqApplication.class, args);
		// BrokerServer 仅在 main() 启动时创建, @SpringBootTest 不会调用 main()
		BrokerServer brokerServer = new BrokerServer(9090);
		brokerServer.start();
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		// 保证 @SpringBootTest 等场景下 context 也被正确赋值
		context = (ConfigurableApplicationContext) applicationContext;
	}
}
