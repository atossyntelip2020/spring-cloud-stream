/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.integration;

import java.util.Random;
import java.util.UUID;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.GenericMessage;

/**
 * Sample spring cloud stream application that demonstrates the usage of {@link SpringIntegrationChannelBinder}.
 *
 * @author Oleg Zhurakousky
 *
 */
@SpringBootApplication
@EnableBinding(Processor.class)
@Import(SpringIntegrationBinderConfiguration.class)
public class SampleStreamApp {

	private static Random random = new Random();

	public static void main(String[] args) throws Exception {
		ApplicationContext context = new SpringApplicationBuilder(SampleStreamApp.class).web(WebApplicationType.NONE)
				.run("--server.port=0");
		Random random = new Random();
		SourceDestination source = context.getBean(SourceDestination.class);
		TargetDestination target = context.getBean(TargetDestination.class);
		for (int i = 0; i < 1000000; i++) {

			source.send(new GenericMessage<byte[]>(new byte[random.nextInt(1000)+1]));
			Thread.sleep(random.nextInt(10));
		}

//		Message<?> message = target.receive();
//		assertEquals("Hello", new String((byte[])message.getPayload(), StandardCharsets.UTF_8));
	}

	@StreamListener(Processor.INPUT)
	@SendTo(Processor.OUTPUT)
//	@ServiceActivator(inputChannel="input", outputChannel="output")
	public String receive(String value) throws  Exception{
		Thread.sleep(random.nextInt(100));
		return (value + UUID.randomUUID()).substring(random.nextInt(value.length()));
	}

	@ServiceActivator(inputChannel="input.anonymous.errors")
	public void error(String value) {
		System.out.println("Handling ERROR payload: " + value);
	}
}


