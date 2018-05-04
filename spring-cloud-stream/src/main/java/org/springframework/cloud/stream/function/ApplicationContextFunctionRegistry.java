package org.springframework.cloud.stream.function;

import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.Message;

/**
 * Implementation of {@link ApplicationContext}-based {@link FunctionRegistry}
 *
 * @author Oleg Zhurakousky
 *
 */
public class ApplicationContextFunctionRegistry implements FunctionRegistry<Message<?>>, ApplicationContextAware {

	protected final Log logger = LogFactory.getLog(this.getClass());

	private ConfigurableApplicationContext applicationContext;

	@SuppressWarnings("rawtypes")
	@Override
	public Map<String, TypeAwareFunction> getFunctions() {
		return this.applicationContext.getBeansOfType(TypeAwareFunction.class);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Map<String, TypeAwareConsumer> getConsumers() {
		return this.applicationContext.getBeansOfType(TypeAwareConsumer.class);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	@Override
	public FunctionInvoker<Message<?>> getConsumerInvoker(TypeAware typeAware) {
		return  new MessagingFunctionInvoker(typeAware);
	}

	@Override
	public FunctionInvoker<Message<?>> getFunctionInvoker(TypeAware typeAware, Consumer<Message<?>> delegate) {
		return new MessagingFunctionInvoker(typeAware, delegate);
	}
}
