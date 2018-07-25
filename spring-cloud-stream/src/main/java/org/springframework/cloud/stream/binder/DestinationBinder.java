package org.springframework.cloud.stream.binder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.messaging.MessageListeningContainer;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.util.CollectionUtils;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public abstract class DestinationBinder<C extends ConsumerProperties, P extends ProducerProperties>
	implements InitializingBean, ApplicationContextAware, ExtendedBindingProperties<C, P> {

	protected final Log logger = LogFactory.getLog(this.getClass());

	private final ProvisioningProvider<C, P> provisioningProvider;

	private ConfigurableApplicationContext applicationContext;

	private final FunctionCatalog functionCatalog;

	private final FunctionInspector functionInspector;

	private final SmartMessageConverter messageConverter;

	public DestinationBinder(ProvisioningProvider<C, P> provisioningProvider, FunctionCatalog functionCatalog,
			FunctionInspector functionInspector, CompositeMessageConverterFactory converterFactory) {
		this.messageConverter = converterFactory != null ? converterFactory.getMessageConverterForAllRegistered() : null;
		this.provisioningProvider = provisioningProvider;
		this.functionCatalog = functionCatalog;
		this.functionInspector = functionInspector;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	protected ConfigurableApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

	/**
	 * Binds functions to {@link MessageListeningContainer}
	 * @throws Exception
	 */
	public void bind() throws Exception {
		List<BindingInfo> bindingInfo = this.computeBindingInfo();

		for (BindingInfo bi : bindingInfo) {
			String inputDestinationName = bi.getInputDestinationName();
			String outputDestinationName = bi.getOutputDestinationName();
			String groupName = bi.getGroupName();

			if (bi.getFunction() instanceof Function) {
				ProducerDestination producerDestination = this.provisioningProvider
						.provisionProducerDestination(outputDestinationName, this.getExtendedProducerProperties(outputDestinationName));
				ConsumerDestination consumerDestination = this.provisioningProvider.provisionConsumerDestination(
						inputDestinationName, groupName, this.getExtendedConsumerProperties(inputDestinationName));

				FunctionalBinding<?, ?> functionBinding = this.bind(consumerDestination.getName(), this.getExtendedConsumerProperties(inputDestinationName),
						producerDestination.getName(), this.getExtendedProducerProperties(outputDestinationName), bi.getFunction());

				this.registerBinding(functionBinding);
			}
			else if (bi.getFunction() instanceof Consumer) {
//				ConsumerDestination consumerDestination = this.provisioningProvider
//						.provisionConsumerDestination(inputDestinationName, bi.getGroupName(), this.getExtendedConsumerProperties(inputDestinationName));
//
//				TypeAwareMessageHandler messageHandler = new TypeAwareMessageHandler(bi.getFunction(), this.functionInspector, this.messageConverter);
//
//				Lifecycle listeningContainer = this.createInboundListener(consumerDestination.getName(), this.getExtendedConsumerProperties(inputDestinationName), messageHandler);
//				ConsumerDestinationBinding consumerBinding = new ConsumerDestinationBinding(consumerDestination, listeningContainer);
//				this.registerBinding(consumerBinding);
			}
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		this.bind();
	}

	protected abstract MessageListeningContainer createMessageListeningContainer(String inputDestinationName, C consumerProperties);

	protected abstract Consumer<Message<?>> createOutboundConsumer(String destinationName, P producerProperties);

	protected abstract void onCommit(Message<?> message);

	/**
	 *
	 */
	private FunctionalBinding<Message<?>, Object> bind(String inputDestinationName, C consumerProperties, String outputDestinationName, P producerProperties, Object function) {
		Consumer<Message<?>> outboundConsumer = this.createOutboundConsumer(outputDestinationName, producerProperties);
		return new SimpleFunctionalBinding(inputDestinationName, consumerProperties, outboundConsumer, function);
	}


	private List<BindingInfo> computeBindingInfo() {
		List<BindingInfo> bindingInfoList = new ArrayList<>();
		Set<String> functions = functionCatalog.getNames(Function.class);
		Set<String> consumers = functionCatalog.getNames(Consumer.class);
		if (!CollectionUtils.isEmpty(functions)) {
			for (String functionName : functions) {
				Object userFunction = functionCatalog.lookup(Function.class, functionName);
				BindingInfo bindingInfo = new BindingInfo(userFunction, functionName);
				bindingInfo.setInputDestinationName(functionName + "_input");
				bindingInfo.setOutputDestinationName(functionName + "_output");
				bindingInfo.setGroupName("reactiveBinder");
				bindingInfoList.add(bindingInfo);
			}
		}
		if (!CollectionUtils.isEmpty(consumers)) {
			for (String consumerName : consumers) {
				Object userConsumer = functionCatalog.lookup(Consumer.class, consumerName);
				BindingInfo bindingInfo = new BindingInfo(userConsumer, consumerName);
				bindingInfo.setInputDestinationName(consumerName + "_input");
				bindingInfo.setGroupName("reactiveBinder");
				bindingInfoList.add(bindingInfo);
			}
		}
		return bindingInfoList;
	}

	private void registerBinding(FunctionalBinding<?, ?> binding) {
		BeanFactory bf = this.applicationContext.getBeanFactory();
		if (bf instanceof ConfigurableListableBeanFactory) {
			((ConfigurableListableBeanFactory)bf).registerSingleton(binding.getName(), binding);
		}
		if (bf instanceof AutowireCapableBeanFactory) {
			((AutowireCapableBeanFactory)bf).initializeBean(binding, binding.getName());
		}
	}

	/**
	 *
	 */
	private class SimpleFunctionalBinding implements FunctionalBinding<Message<?>, Object> {
		private final Disposable.Composite disposables = Disposables.composite();

		private final Supplier<MessageListeningContainer> messageListeningContainerSupplier;

		private final Consumer<Message<?>> outboundConsumer;

		private final String inputDestinationName;

		private final Object function;

		private final C consumerProperties;

		public SimpleFunctionalBinding(String inputDestinationName, C consumerProperties, Consumer<Message<?>> outboundConsumer, Object function) {
			this.messageListeningContainerSupplier =
					() -> createMessageListeningContainer(inputDestinationName, consumerProperties);
			this.outboundConsumer = outboundConsumer;
			this.inputDestinationName = inputDestinationName;
			this.function = function;
			this.consumerProperties = consumerProperties;
		}

		@Override
		public Publisher<Message<?>> inputPublisher() {
			return Flux.create(emitter -> {
				logger.info("Creating MessageListeningContainer");
				MessageListeningContainer container = messageListeningContainerSupplier.get();
				Disposable d = () -> container.stop();
				disposables.add(d);
				emitter.onDispose(d); // will only be invoked in non-caching scenarios
				container.setListener(message -> {
						if (message == null) {
							disposables.remove(d);
							d.dispose();
							emitter.complete(); // will shut down the publisher and will call the above onDisoose()
						}
						else {
							emitter.next(message);
						}
					});
				container.start();
			});
		}

		@Override
		public Mono<Void> outputPublisher(Publisher<Object> outputPublisher) {
			return outboundConsumer == null
					? Flux.from(outputPublisher).then()
							: Flux.from(outputPublisher).doOnNext(message -> {
									System.out.println("OUTGOING: " + message);
									outboundConsumer.accept((Message<?>) message);
								}).then();
		}

		@Override
		public void onError(Throwable error) {
			logger.error("Failed to process messages", error); // This is where "Retries exhausted: 3/3. . ." will be logged
			onComplete();
		}

		public void onComplete() {
			disposables.dispose();
		}

		@Override
		public String getName() {
			return inputDestinationName;
		}

		@Override
		public Disposable subscribe() {

			int windowSize = 1;
			Duration windowDuration = Duration.ofSeconds(60);

			int retryCount = 3;
			Duration retryBackoffDuration = Duration.ofSeconds(1);

			boolean fluxOfluxFunction = false;
			boolean pubSub = false;

			FunctionInvoker functionInvoker = new FunctionInvoker(function, functionInspector, messageConverter);
			Flux<Message<?>> inputPublisher = Flux.from(inputPublisher());
			if (pubSub) {
				inputPublisher = inputPublisher.publish().refCount(1);
			}
			Disposable disposableSubscription = null;
			if (fluxOfluxFunction) {  // YOLO
//				Object fluxOfFluxObject = windowFunction(inputPublisher);
//				Flux<Flux> fluxOfFlux = (Flux<Flux>) fluxOfFluxObject;
//				fluxOfFlux.flatMap(window -> b.outputPublisher(window))
//				.subscribe();
			}
			else if (windowSize > 1) { //(should be > only, see else)  MICROBATCH
				disposableSubscription = inputPublisher
						.windowTimeout(windowSize, windowDuration)
						.concatMap(window -> {
							Message<?>[] last = new Message[1];
							return window.cache()
										.doOnCancel(() -> onComplete()) // will call on complete where disposables will be invoked and container shut down
										.doOnNext(msg -> {last[0] = msg;})
										.transform(flux -> functionInvoker.apply(flux)) // invoke user function
										.as(publisher -> outputPublisher(publisher)) // send output, using as vs transform because outputPublisher returns a Mono not a Flux
										.doOnSuccess(noop -> onCommit(last[0]))
										.retryBackoff(retryCount, retryBackoffDuration); // sideeffect;
						})
						.subscribe(d -> {}, exception -> onError(exception));
			}
			else {
				disposableSubscription = outputPublisher(functionInvoker.apply(inputPublisher).retryBackoff(retryCount, retryBackoffDuration))
						.subscribe(d -> {}, exception -> onError(exception));
			}

			return disposableSubscription;
		}
	};

}
