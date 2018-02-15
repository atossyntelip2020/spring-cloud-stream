/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.binding;

import java.nio.charset.StandardCharsets;

import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;

/**
 * An implementation of {@link BindingTargetFactory} for creating
 * {@link SubscribableChannel}s.
 *
 * @author Marius Bogoevici
 * @author David Syer
 * @author Ilayaperumal Gopinathan
 */
public class SubscribableChannelBindingTargetFactory extends AbstractBindingTargetFactory<SubscribableChannel> implements EnvironmentAware {

	private final MessageChannelConfigurer messageChannelConfigurer;
	
	private Environment environment;

	public SubscribableChannelBindingTargetFactory(MessageChannelConfigurer messageChannelConfigurer) {
		super(SubscribableChannel.class);
		this.messageChannelConfigurer = messageChannelConfigurer;
	}
	
	private static class ToTextConverter implements MessageConverter {
		@Override
		public Object fromMessage(Message<?> message, Class<?> targetClass) {
			String contentType = message.getHeaders().containsKey(MessageHeaders.CONTENT_TYPE) 
					? message.getHeaders().get(MessageHeaders.CONTENT_TYPE).toString() 
							: BindingProperties.DEFAULT_CONTENT_TYPE;
			if (contentType.contains("text") || contentType.contains("json")) {
				message = MessageBuilder.withPayload(new String(((byte[])message.getPayload()), StandardCharsets.UTF_8)).copyHeaders(message.getHeaders()).build();
			}
			return message;
		}

		@Override
		public Message<?> toMessage(Object payload, MessageHeaders headers) {
			throw new UnsupportedOperationException("This converter does not support this method");
		}
	}

	@Override
	public SubscribableChannel createInput(String name) {
		DirectChannel subscribableChannel = new DirectChannel();
		boolean inputAsText = this.environment.getProperty("spring.cloud.stream.input-as-text", Boolean.class, false);
		if (inputAsText) {
			subscribableChannel.setDatatypes(String.class);
			subscribableChannel.setMessageConverter(new ToTextConverter());
		}
		this.messageChannelConfigurer.configureInputChannel(subscribableChannel, name);
		return subscribableChannel;
	}

	@Override
	public SubscribableChannel createOutput(String name) {
		SubscribableChannel subscribableChannel = new DirectChannel();
		this.messageChannelConfigurer.configureOutputChannel(subscribableChannel, name);
		return subscribableChannel;
	}

	@Override
	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}
}
