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

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.newbinder.ext.ProvenanceProvider;
import org.springframework.core.ResolvableType;
import org.springframework.core.env.Environment;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * Core binder that delegates to available {@link Binding}s to configure
 * target {@link Message} 'sender' and 'receiver'.
 *
 * @author Oleg Zhurakousky
 *
 * @see AbstractSenderBinding
 * @see AbstractReceiverBinding
 */
class StreamBinder<P extends ProducerProperties, C extends ConsumerProperties> extends AbstractStreamLiveCycle implements InitializingBean {

	private final Logger logger = LoggerFactory.getLogger(StreamBinder.class);

	private boolean outputProducingConsumerExists;

	private AbstractSenderBinding<P,C> senderBinding;

	private AbstractReceiverBinding<P,C> receiverBinding;

	// ===== Autowire configuration =====
	@Autowired
	private BindingServiceProperties bindingServiceProperties;

	@Autowired
	private CompositeMessageConverter streamMessageConverter;

	@Autowired
	private AbstractSenderBinding<P,C>[] availableSenderBinding;

	@Autowired
	private AbstractReceiverBinding<P,C>[] availableReceiverBinding;

	@Autowired(required=false)
	private List<ConsumerWrapper> consumers;

	@Autowired(required=false)
	private List<ProducerWrapper> producers;

	@Autowired
	private Environment environment;// TODO is it needed?

	@Autowired(required=false)
	private ProvenanceProvider provenanceProvider;

	/**
	 *
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		outputProducingConsumerExists = this.consumers.stream()
			.filter(cWrapper ->!cWrapper.getGenericParameters()[1].getRawClass().equals(Void.class))
			.findFirst()
			.isPresent();
	}

	/**
	 *
	 */
	@Override
	public void doStart() {
		// Registers Bindings to be used by sender/receiver
		if (this.isConsumerPresent()){
			this.receiverBinding = this.determineBinding(true);
			logger.info("Registering RECEIVER binding: " + this.receiverBinding);
		}
		if (this.receiverBinding == null){
			logger.info("RECEIVER binding was not registered since no consumers are present in the application.");
		}

		// TODO what if only producers present??? Revisit
		if ((this.isConsumerPresent() && this.isProducerPresent()) || this.bindingServiceProperties.isCreateSenderBinding()){
			this.senderBinding = this.determineBinding(false);
			logger.info("Registering SENDER binding: " + this.senderBinding);
		}
		if (this.senderBinding == null){
			logger.info("SENDER binding was not registered since no producers are present in the application and create-sender-binding is not set.");
		}

		Consumer<Message<byte[]>> sender = this.senderBinding != null ? this.senderBinding.getSender() : null;;

		DelegatingConsumer bindingConsumer = new DelegatingConsumer(sender);
		if (this.receiverBinding != null) { // can be null if there are no consumers
			this.receiverBinding.registerReceiver(bindingConsumer);
		}

		if (this.senderBinding != null) {
			this.senderBinding.start();
		}
		if (this.receiverBinding != null) {
			this.receiverBinding.start();
		}
	}

	/**
	 *
	 */
	@Override
	public void doStop() {
		if (this.senderBinding != null) {
			this.senderBinding.stop();
		}
		if (this.receiverBinding != null) {
			this.receiverBinding.stop();
		}
		this.receiverBinding = null;
		this.senderBinding = null;
	}

	/**
	 *
	 */
	@Override
	public boolean isAutoStartup() {
		return true;
	}

	//TODO
	// Can't find references to spring.cloud.stream.bindings.input/output.binder properties in stream core. Are they used?
	/**
	 * Determines which binding to use in the event there are more then a
	 * single {@link Binding} provider on the classpath (e.g., rabbit, kafka).<br>
	 * It relies on 'spring.cloud.stream.bindings.input.binder',
	 * 'spring.cloud.stream.bindings.output.binder' and 'spring.cloud.stream.defaultBinder' properties.
	 * @param receiver boolean specifying if it should use receiver or sender
	 * Binding from the selected {@link Binding} provider.
	 *
	 * @return {@link Binding} to be used
	 */
	@SuppressWarnings("unchecked")
	private <B extends Binding> B determineBinding(boolean receiver) {
		// TODO look at the BindingServiceProperties. There is property for Default but no input/output
		B rb;
		B[] availableBinders = receiver ? (B[])this.availableReceiverBinding : (B[])this.availableSenderBinding;
		if (availableBinders.length > 1) {
			String specificBinder = receiver
					? environment.getProperty("spring.cloud.stream.bindings.input.binder")
							: environment.getProperty("spring.cloud.stream.bindings.output.binder");
			String binderToUse = StringUtils.hasText(specificBinder) ? specificBinder : this.bindingServiceProperties.getDefaultBinder();
			Assert.isTrue(StringUtils.hasText(binderToUse), "Multiple binders detected in the classpath: "
					+ Stream.of(this.availableReceiverBinding).map(b -> b.getName()).collect(Collectors.toList())
					+ ". You must provide the name of the binder to use using one of '"
					+ (receiver ? "spring.cloud.stream.bindings.input.binder" : "spring.cloud.stream.bindings.output.binder")
					+ "' or 'spring.cloud.stream.defaultBinder' properties.");
			rb = (B) Stream.of(this.availableReceiverBinding).filter(b -> b.getName().equals(binderToUse)).findFirst().get();
		}
		else {
			rb = (B) (receiver ? this.availableReceiverBinding[0] : this.availableSenderBinding[0]);
		}
		return rb;
	}

	/**
	 * Returns true if at least one consumer is present
	 */
	private boolean isConsumerPresent() {
		return !CollectionUtils.isEmpty(this.consumers);
	}

	/**
	 *
	 */
	private boolean isProducerPresent() {
		return !CollectionUtils.isEmpty(this.producers) || this.outputProducingConsumerExists;
	}

	/**
	 * Consumer which invokes Function while also providing a bridge between
	 * receiverBinding (function input) and senderBinding (function output).
	 */
	private final class DelegatingConsumer implements Consumer<Message<byte[]>> {

		private final Consumer<Message<byte[]>> sender;

		DelegatingConsumer(Consumer<Message<byte[]>> sender) {
			this.sender = sender;
		}

		/**
		 * NOTE: Take extra caution in the fact that the inbound {@link Message} was created
		 * with mutable headers.
		 * If need to update/add message headers by calling {@link MessageHeaderAccessor}
		 * <pre>
		 * MessageHeaderAccessor a = MessageHeaderAccessor.getMutableAccessor(rawData);
		 * a.setHeader(..)
		 * </pre>
		 */
		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public void accept(Message<byte[]> inboundMessage) {
			this.captureProvenanceEvent(inboundMessage, true);
			MessageHeaders messageHeaders = inboundMessage.getHeaders();

			ResolvableType inputType = null;
			Object resolvedInputParameter = null;
			for (ConsumerWrapper consumerWrapper : StreamBinder.this.consumers) {
				boolean inputTypeIsMessage = false;
				if (!consumerWrapper.getGenericParameters()[0].equals(inputType)) {
					inputType = consumerWrapper.getGenericParameters()[0];

					Class<?> convertableType = inputType.getRawClass();
					if (convertableType.isAssignableFrom(Message.class)) {
						inputTypeIsMessage = true;
						ResolvableType payloadType = inputType.getGeneric();
						convertableType = payloadType.getRawClass();
					}
					resolvedInputParameter = StreamBinder.this.streamMessageConverter.fromMessage(inboundMessage, convertableType);
					Assert.notNull(resolvedInputParameter, "Failed to convert input parameter to '" + convertableType.getName() + "'. No suitable converter found.");
				}
				if (inputTypeIsMessage) {
					resolvedInputParameter = MessageBuilder.withPayload(resolvedInputParameter).copyHeaders(messageHeaders).build();
				}

				// === invoke function
				Object resultValue = ((Function)consumerWrapper.getConsumer()).apply(resolvedInputParameter);
				// ===================

				if (resultValue != null) {
					//TODO ensure that converters create Message with mutable headers so additional data (i.e., provenance) could be added without modifying message
					if (resultValue instanceof Message) {
						messageHeaders = ((Message<?>)resultValue).getHeaders();
						resultValue = ((Message<?>)resultValue).getPayload();
					}
					Message<byte[]> outboundMessage = (Message<byte[]>) StreamBinder.this.streamMessageConverter.toMessage(resultValue, messageHeaders);

					// === send downstream
					this.sender.accept(outboundMessage);
					//====================

					this.captureProvenanceEvent(outboundMessage, false);
				}
			}
		}

		/**
		 *
		 */
		private void captureProvenanceEvent(Message<byte[]> rawData, boolean inbound) {
			if (StreamBinder.this.provenanceProvider != null) {
				logger.warn("Provenance event capture is not supported at the moment");
			}
		}
	}
}
