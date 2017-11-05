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
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
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
import org.springframework.core.env.Environment;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConverter;
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
	private Map<Function<?, ?>, Type[]> typedConsumers;

	private Map<Supplier<?>, Type> typedSuppliers;
	// ----

	private boolean atLeastOneFunctionWithNonVoidReturn;

	private AbstractSenderBinding<P,C> senderBinding;

	private AbstractReceiverBinding<P,C> receiverBinding;

	private boolean nonVoidFunctionExists;

	// ===== Autowire configuration =====
	@Autowired
	private BindingServiceProperties bindingServiceProperties;

	@Autowired
	private DefaultListableBeanFactory beanFactory;

	@Autowired
	private MessageConverter streamMessageConverter;

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
	}

	/**
	 *
	 */
	@Override
	public void doStart() {
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
	public void registerFunction(String name, Function<?, ?> function, Type[] signature) {
		this.typedConsumers.put(function, signature);
		this.logFunctionRegistration(name, signature);
	}

	/**
	 *
	 */
	public void registerSupplier(String name, Supplier<?> supplier, Type signature) {
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
	private Type[] discoverArgumetsType(String beanName){
		RootBeanDefinition beanDefinition = (RootBeanDefinition) beanFactory.getMergedBeanDefinition(beanName);
		Method method = beanDefinition.getResolvedFactoryMethod();
		Class<?> functionalInterface = method.getReturnType().isAssignableFrom(Function.class)
				? Function.class : (method.getReturnType().isAssignableFrom(Consumer.class)
						? Consumer.class : Supplier.class);
		Type[] types = retrieveTypes(method.getGenericReturnType(), functionalInterface);

		/*
		 * Asserts that there is 0 or 1 {@link Function} bean.
		 * Functions are a special case since they would act as an implicit splitter,
		 * and without matching aggregator this could create a confusion by
		 * producing multiple messages from a single input.
		 *
		 * On the flip side if user's intention is to execute all functions in some
		 * order, they should compose them into a single function annotated with @Bean
		 */
		if (types.length == 2 && !Void.class.isAssignableFrom((Class<?>) types[1])) {
			if (this.nonVoidFunctionExists) {
				throw new IllegalStateException("Discovered more then one Function with non-Void return type. This is not allowed. "
						+ "If the intention was to invoke multiple Functions in certain order please compose them with `andThen` "
						+ "into a single Function bean.");
			}
			this.nonVoidFunctionExists = true;
		}
		if (functionalInterface.isAssignableFrom(Consumer.class)) {
			types = new Type[] {types[0], Void.class};
		}
		this.logFunctionRegistration(beanName, types);
		if (functionalInterface.isAssignableFrom(Function.class) && !types[1].equals(Void.class)){
			this.atLeastOneFunctionWithNonVoidReturn = true;
		}
		return types;
	}

	/**
	 *
	 */
	//TODO improve error message
	private Type[] retrieveTypes(Type genericInterface, Class<?> interfaceClass){
		if ((genericInterface instanceof ParameterizedType) && interfaceClass
				.getTypeName().equals(((ParameterizedType) genericInterface).getRawType().getTypeName())) {
			ParameterizedType type = (ParameterizedType) genericInterface;
			return type.getActualTypeArguments();
		}
		throw new IllegalStateException("Failed to discover Function signature");
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
		return !CollectionUtils.isEmpty(this.typedSuppliers) || this.atLeastOneFunctionWithNonVoidReturn;
	}

	/**
	 *
	 */
	private void logFunctionRegistration(String name, Type... signature) {
		if (logger.isDebugEnabled()){
			String mapping = Arrays.asList(signature).toString().replaceAll("\\[", "").replaceAll("]", "");
			String functionalInterfaceName = (signature.length == 2 ? "Function" : "Supplier");
			if (StringUtils.hasText(name)) {
				logger.debug("Registered " + functionalInterfaceName + "; " + name + "(" + mapping + ")");
			}
			else {
				logger.debug("Registered " + functionalInterfaceName + "; (" + mapping + ")");
			}
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
		public void accept(Message<byte[]> rawData) {
			this.captureProvenanceEvent(rawData, true);
			Type inputType = null;
			Object functionArgument = null;
			for (Entry<Function<?,?>, Type[]> entry : StreamBinder.this.typedConsumers.entrySet()) {
				if (!entry.getValue()[0].equals(inputType)) {
					inputType = entry.getValue()[0];
					functionArgument = StreamBinder.this.streamMessageConverter.fromMessage(rawData, (Class<?>) inputType);
				}
				Object resultValue = ((Function)entry.getKey()).apply(functionArgument);
				if (resultValue != null) {
					//TODO ensure that converters create Message with mutable headers so additional data (i.e., provenance) could be added without modifying message
					Message<byte[]> resultMessage = (Message<byte[]>) StreamBinder.this.streamMessageConverter.toMessage(resultValue, rawData.getHeaders());
					this.sender.accept(resultMessage);
					this.captureProvenanceEvent(resultMessage, false);
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
