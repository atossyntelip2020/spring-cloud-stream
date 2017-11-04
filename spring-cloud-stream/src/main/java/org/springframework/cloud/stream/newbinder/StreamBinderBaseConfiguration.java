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
package org.springframework.cloud.stream.newbinder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.GenericMessage;

@Configuration
@EnableConfigurationProperties({BindingServiceProperties.class})
class StreamBinderBaseConfiguration {

	@Bean
	public MessageConverter streamMessageConverter() {
		List<MessageConverter> messageConverters = new ArrayList<>();
		messageConverters.add(new ByteArrayToStringConverter());
		StrictCompositeMessageConverter messageConverter = new StrictCompositeMessageConverter(messageConverters);

		return messageConverter;
	}

	@Bean
	public <P extends ProducerProperties, C extends ConsumerProperties> StreamBinder<P,C> streamBinder() {
		return new StreamBinder<>();
	}

	/*
	 * TODO
	 * Consider supporting byte[] to/from primitives such as Integer, Long etc.
	 * Basically whatever ByteBufer supports
	 */
	private static class ByteArrayToStringConverter implements MessageConverter {

		@Override
		public Object fromMessage(Message<?> message, Class<?> targetClass) {
			Object returnedValue = null;
			if (!message.getHeaders().containsKey(MessageHeaders.CONTENT_TYPE)
					&& targetClass.isAssignableFrom(String.class) && message.getPayload() instanceof byte[]) {
				returnedValue = new String((byte[])message.getPayload(), StandardCharsets.UTF_8);
			}
			return returnedValue;
		}

		@Override
		public Message<?> toMessage(Object payload, MessageHeaders headers) {
			Message<byte[]> message = null;
			if (!headers.containsKey(MessageHeaders.CONTENT_TYPE)) {
				if (payload instanceof byte[]) {
					message = new GenericMessage<byte[]>((byte[]) payload, headers);
				}
				else if (payload instanceof String) {
					message = new GenericMessage<byte[]>(((String) payload).getBytes(StandardCharsets.UTF_8), headers);
				}
			}
			return message;
		}

	}
}
