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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
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

	// Holds type mappings for all functional beans (Supplier,Function,Consumer) gathered by this binder
	private Map<Function<?, ?>, ResolvableType[]> typedConsumers;

	private Map<Supplier<?>, ResolvableType> typedSuppliers;
	// ----

	private boolean outputProducingConsumerExists;

	private AbstractSenderBinding<P,C> senderBinding;

	private AbstractReceiverBinding<P,C> receiverBinding;

	// ===== Autowire configuration =====
	@Autowired
	private BindingServiceProperties bindingServiceProperties;

	@Autowired
	private DefaultListableBeanFactory beanFactory;

	@Autowired
	private CompositeMessageConverter streamMessageConverter;

	@Autowired
	private AbstractSenderBinding<P,C>[] availableSenderBinding;

	@Autowired
	private AbstractReceiverBinding<P,C>[] availableReceiverBinding;

	@Autowired(required=false)
	protected Map<String, Function<?,?>> consumers;

	@Autowired(required=false)
	private Map<String, Supplier<?>> suppliers;

	@Autowired
	private Environment environment;// TODO is it needed?

	@Autowired(required=false)
	private ProvenanceProvider provenanceProvider;

	/**
	 *
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		// MAP typed suppliers/functions/consumers
		this.initializeTypedProducersAndConsumers();
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

		FunctionInvokingConsumer bindingConsumer = new FunctionInvokingConsumer(sender);
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

	//TODO there will probably be synchronization issues when iteration is performed and addition is made
	/**
	 *
	 */
	public void registerFunction(String name, Function<?, ?> function, ResolvableType[] signature) {
		this.typedConsumers.put(function, signature);
		this.logFunctionRegistration(name, signature);
	}

	/**
	 *
	 */
	public void registerSupplier(String name, Supplier<?> supplier, ResolvableType signature) {
		this.typedSuppliers.put(supplier, signature);
		this.logFunctionRegistration(name, signature);
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
	 * Discovers the argument types for provided {@link Function}s, {@link Consumer}s and {@link Supplier}s
	 */
	private ResolvableType[] discoverArgumetsType(String beanName){
		RootBeanDefinition beanDefinition = (RootBeanDefinition) beanFactory.getMergedBeanDefinition(beanName);
		Method method = beanDefinition.getResolvedFactoryMethod();
		ResolvableType returnType = ResolvableType.forMethodReturnType(method);
		ResolvableType[] returnTypeGenerics = returnType.getGenerics();

		Class<?> convertableType = returnTypeGenerics[0].getRawClass();
		if (convertableType.isAssignableFrom(Message.class)) {
			ResolvableType payloadType = returnTypeGenerics[0].getGeneric();
			convertableType = payloadType.getRawClass();
			Assert.notNull(convertableType, "Failed to determine payload type of the "
					+ "Message identified as an input parameter of function identified as '" + beanName + "', "
					+ "since it is '?'. Please specify a concrete type. In the event you want to receive raw data "
					+ "as the payload use 'Message<byte[]>'.");
		}

		if (returnTypeGenerics.length > 1 && !Void.class.isAssignableFrom(returnTypeGenerics[1].getRawClass())) {
			Assert.isTrue(!this.outputProducingConsumerExists, "Discovered more then one Function with non-Void return type. This is not allowed. "
						+ "If the intention was to invoke multiple Functions in certain order please compose them with `andThen` "
						+ "into a single Function bean.");
			this.outputProducingConsumerExists = true;
		}

		if (returnType.getRawClass().isAssignableFrom(Consumer.class)) {
			returnTypeGenerics = new ResolvableType[] {returnTypeGenerics[0], ResolvableType.forClass(Void.class)};
		}

		this.logFunctionRegistration(beanName, returnTypeGenerics);
		return returnTypeGenerics;
	}

	/**
	 * Sets up maps of suppliers/functions/consumers where each entry
	 * mapping supplier/function/consumer -> Type[].
	 */
	private void initializeTypedProducersAndConsumers() {
		if (!CollectionUtils.isEmpty(this.suppliers)) {
			this.typedSuppliers = this.suppliers.entrySet().stream()
					.collect(Collectors.toMap(e -> e.getValue(), e -> discoverArgumetsType(e.getKey())[0]));
		}

		if (!CollectionUtils.isEmpty(this.consumers)) {
			this.typedConsumers = this.consumers.entrySet().stream()
					.collect(Collectors.toMap(e -> e.getValue(), e -> discoverArgumetsType(e.getKey())));
		}
	}

	/**
	 * Returns true if at least one consumer is present
	 */
	private boolean isConsumerPresent() {
		return !CollectionUtils.isEmpty(this.typedConsumers);
	}

	/**
	 *
	 */
	private boolean isProducerPresent() {
		return !CollectionUtils.isEmpty(this.typedSuppliers) || this.outputProducingConsumerExists;
	}

	/**
	 *
	 */
	private void logFunctionRegistration(String name, ResolvableType... signature) {
		if (logger.isDebugEnabled()){
			String mapping = Arrays.asList(signature).toString().replaceAll("\\[", "").replaceAll("]", "");
			String functionalInterfaceName = (signature.length == 2 ? "Function" : "Supplier");
			logger.debug("Registered " + functionalInterfaceName + "; " + (StringUtils.hasText(name) ? name : "") + "(" + mapping + ")");
		}
	}

	/**
	 * Consumer which invokes Function while also providing a bridge between
	 * receiverBinding (function input) and senderBinding (function output).
	 */
	private final class FunctionInvokingConsumer implements Consumer<Message<byte[]>> {

		private final Consumer<Message<byte[]>> sender;

		FunctionInvokingConsumer(Consumer<Message<byte[]>> sender) {
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
			for (Entry<Function<?,?>, ResolvableType[]> entry : StreamBinder.this.typedConsumers.entrySet()) {
				boolean inputTypeIsMessage = false;
				if (!entry.getValue()[0].equals(inputType)) {
					inputType = entry.getValue()[0];

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
				Object resultValue = ((Function)entry.getKey()).apply(resolvedInputParameter);
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
