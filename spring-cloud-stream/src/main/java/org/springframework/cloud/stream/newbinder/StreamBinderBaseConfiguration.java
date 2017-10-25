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

import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MessageConverter;

@Configuration
class StreamBinderBaseConfiguration {

	@Bean
	public MessageConverter streamMessageConverter() {
		CompositeMessageConverter streamMessageConverter = new CompositeMessageConverterFactory().getMessageConverterForAllRegistered();
		return streamMessageConverter;
	}

	@Bean
	public <P extends ProducerProperties, C extends ConsumerProperties> StreamBinder<P,C> streamBinder() {
		return new StreamBinder<>();
	}
}
