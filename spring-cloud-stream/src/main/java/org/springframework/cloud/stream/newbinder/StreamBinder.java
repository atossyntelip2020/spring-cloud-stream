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
import org.springframework.core.env.Environment;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
class StreamBinder<P extends ProducerProperties, C extends ConsumerProperties> extends AbstractStreamLiveCycle implements InitializingBean {

	private final Logger logger = LoggerFactory.getLogger(StreamBinder.class);

	/*
	 * Holds type mappings for all functional beans (Supplier,Function,Consumer) gathered by this binder
	 */
	private Map<Function<?, ?>, Type[]> typedFunctions;

	private Map<Consumer<?>, Type> typedConsumers;

	private Map<Supplier<?>, Type> typedSuppliers;

	private boolean atLeastOneFunctionWithNonVoidReturn;

	private AbstractSenderBinding<P,C> senderBinding;

	private AbstractReceiverBinding<P,C> receiverBinding;

	// ===== Autowire configuration =====
	@Autowired
	private DefaultListableBeanFactory beanFactory;

	@Autowired
	private MessageConverter streamMessageConverter;

	@Autowired
	private AbstractSenderBinding<P,C>[] availableSenderBinding;

	@Autowired
	private AbstractReceiverBinding<P,C>[] availableReceiverBinding;

	@Autowired(required=false)
	protected Map<String, Function<?,?>> functions;

	@Autowired(required=false)
	private Map<String, Consumer<?>> consumers;

	@Autowired(required=false)
	private Map<String, Supplier<?>> suppliers;

	@Autowired
	private Environment environment;

	/**
	 *
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		this.assertZeroOrOneFunction();

		//!!!!!!!!!!!!!
		boolean createSenderBinding = false;
//		Basically the message shoudl eihter be dropped or sent to destination (existing)
//		Will work fine with Rabbit, but need a more generic solution
		//!!!!!!!!!!!!

		// MAP typed suppliers/functions/consumers
		this.initializeTypedProducersConsumers();

		// Registers Bindings to be used by sender/receiver
		if (this.isConsumerPresent()){
			this.receiverBinding = this.determineBinding(true);
			logger.info("Registering RECEIVER binding: " + this.receiverBinding);
		}
		if (this.receiverBinding == null){
			logger.info("RECEIVER binding was not registered since no consumers are present in the application.");
		}

		if ((this.isConsumerPresent() && this.isProducerPresent()) || createSenderBinding){
			this.senderBinding = this.determineBinding(false);
			logger.info("Registering SENDER binding: " + this.senderBinding);
		}
		if (this.senderBinding == null){
			logger.info("SENDER binding was not registered since no producers are present in the application and create-sender-binding is not set.");
		}

		Consumer<Message<byte[]>> sender = this.senderBinding != null ? this.senderBinding.getSender() : null;;

		BindingConsumer bindingConsumer = new BindingConsumer(sender);
		this.receiverBinding.registerReceiver(bindingConsumer);
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

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	/**
	 *
	 */
	private final class BindingConsumer implements Consumer<Message<byte[]>> {

		private final Consumer<Message<byte[]>> sender;

		BindingConsumer(Consumer<Message<byte[]>> sender) {
			this.sender = sender;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public void accept(Message<byte[]> rawData) {

			if (StreamBinder.this.typedConsumers != null){
				for (Entry<Consumer<?>, Type> consumerEntry : StreamBinder.this.typedConsumers.entrySet()) {
					Object argument = streamMessageConverter.fromMessage(rawData, (Class<?>) consumerEntry.getValue());
					((Consumer)consumerEntry.getKey()).accept(argument);
				}
			}
			if (StreamBinder.this.typedFunctions != null){
				for (Entry<Function<?,?>, Type[]> functionEntry : StreamBinder.this.typedFunctions.entrySet()) {
					Object argument = streamMessageConverter.fromMessage(rawData, (Class<?>) functionEntry.getValue()[0]);
					Object result = ((Function)functionEntry.getKey()).apply(argument);
					Message<byte[]> resultMessage = (Message<byte[]>) streamMessageConverter.toMessage(result, rawData.getHeaders());
					this.sender.accept(resultMessage);
				}
			}
		}
	}

	/*
	 * TODO
	 * This is something to think about with regard to registering consumers/functions/suppliers dynamically
	 * The issue is that unless they are beans we need to somehow pass type information
	 * Once that is done than it has to go thru the same routine where
	 * 	- if it happen to be the first function, then provision destination, create binding etc.
	 */
	public void registerFunctional(Object functional) {
		throw new UnsupportedOperationException();
	}

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
		B rb;
		B[] availableBinders = receiver ? (B[])this.availableReceiverBinding : (B[])this.availableSenderBinding;
		if (availableBinders.length > 1) {
			String specificBinder = receiver
					? environment.getProperty("spring.cloud.stream.bindings.input.binder")
							: environment.getProperty("spring.cloud.stream.bindings.output.binder");
			String binderToUse = StringUtils.hasText(specificBinder) ? specificBinder : environment.getProperty("spring.cloud.stream.defaultBinder");
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
				? Function.class : (method.getReturnType().isAssignableFrom(Consumer.class) ? Consumer.class : Supplier.class);
		Type[] types = retrieveTypes(method.getGenericReturnType(), functionalInterface);
		if (logger.isDebugEnabled()){
			logger.debug("Added type mappings: " + beanName + "(" + Arrays.asList(types).toString().replaceAll("\\[", "").replaceAll("]", "") + ")");
		}
		if (functionalInterface.isAssignableFrom(Function.class) && !types[1].equals(Void.class)){
			this.atLeastOneFunctionWithNonVoidReturn = true;
		}
		return types;
	}

	/**
	 *
	 */
	private Type[] retrieveTypes(Type genericInterface, Class<?> interfaceClass){
		if ((genericInterface instanceof ParameterizedType) && interfaceClass
				.getTypeName().equals(((ParameterizedType) genericInterface).getRawType().getTypeName())) {
			ParameterizedType type = (ParameterizedType) genericInterface;
			Type[] args = type.getActualTypeArguments();
			return args;
		}
		return null;
	}

	/**
	 * Sets up maps of suppliers/functions/consumers where each entry
	 * mapping supplier/function/consumer -> Type[].
	 */
	private void initializeTypedProducersConsumers() {
		if (!CollectionUtils.isEmpty(this.suppliers)) {
			this.typedSuppliers = this.suppliers.entrySet().stream()
					.collect(Collectors.toMap(e -> e.getValue(), e -> discoverArgumetsType(e.getKey())[0]));
		}

		if (!CollectionUtils.isEmpty(this.functions)) {
			this.typedFunctions = this.functions.entrySet().stream()
					.collect(Collectors.toMap(e -> e.getValue(), e -> discoverArgumetsType(e.getKey())));
		}

		if (!CollectionUtils.isEmpty(this.consumers)) {
			this.typedConsumers = this.consumers.entrySet().stream()
					.collect(Collectors.toMap(e -> e.getValue(), e -> discoverArgumetsType(e.getKey())[0]));
		}
	}

	/**
	 * Returns true if at least one consumer is present
	 */
	private boolean isConsumerPresent() {
		return !CollectionUtils.isEmpty(this.typedConsumers) || !CollectionUtils.isEmpty(this.typedFunctions);
	}

	/**
	 *
	 */
	private boolean isProducerPresent() {
		return !CollectionUtils.isEmpty(this.typedSuppliers) || this.atLeastOneFunctionWithNonVoidReturn;
	}

	/**
	 * Asserts that that there is 0 or 1 {@link Function}.<br>
	 * Function is a special case since they will act as an implicit splitter,
	 * and without matching aggregator this could create a confusion by
	 * producing multiple messages from a single input.
	 * <br>
	 * On the flip side if user's intention is to execute all functions in some
	 * order, they should compose them into a single function annotated with @Bean
	 */
	private void assertZeroOrOneFunction() {
		if (this.functions != null) {
			Assert.isTrue(functions.size() <= 1, "Found more then one Function configured as @Bean which is not allowed. If "
					+ "the intention was to invoke them in certain order please compose them with `andThen`");
		}
	}
}
