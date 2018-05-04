package org.springframework.cloud.stream.function;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class TypeAwareBeanPostProcessor implements BeanPostProcessor, BeanFactoryAware {

	private ConfigurableBeanFactory beanFactory;

	protected final Log logger = LogFactory.getLog(this.getClass());

	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		if (bean instanceof Function || bean instanceof Consumer || bean instanceof Supplier) {
			bean = this.postProcessWithTypeMappings(beanName, bean);
		}
		return bean;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Object postProcessWithTypeMappings(String beanName, Object functionalBean){
		RootBeanDefinition beanDefinition = (RootBeanDefinition) this.beanFactory.getMergedBeanDefinition(beanName);
		Method method = beanDefinition.getResolvedFactoryMethod();
		Class<?> functionalInterface = method.getReturnType().isAssignableFrom(Function.class)
				? Function.class : (method.getReturnType().isAssignableFrom(Consumer.class) ? Consumer.class : Supplier.class);
		Type[] types = retrieveTypes(method.getGenericReturnType(), functionalInterface);
		if (logger.isDebugEnabled()){
			logger.debug("Added type mappings: " + beanName + "(" + Arrays.asList(types).toString().replaceAll("\\[", "").replaceAll("]", "") + ")");
		}

		if (functionalInterface.isAssignableFrom(Function.class)) {
			functionalBean = new TypeAwareFunction((Function) functionalBean, types[0], types[1]);
		}
		else if (functionalInterface.isAssignableFrom(Consumer.class)) {
			functionalBean = new TypeAwareConsumer((Consumer) functionalBean, types[0]);
		}
		return functionalBean;
	}

	private Type[] retrieveTypes(Type genericInterface, Class<?> interfaceClass){
		if ((genericInterface instanceof ParameterizedType) && interfaceClass
				.getTypeName().equals(((ParameterizedType) genericInterface).getRawType().getTypeName())) {
			ParameterizedType type = (ParameterizedType) genericInterface;
			Type[] args = type.getActualTypeArguments();
			return args;
		}
		return null;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = (ConfigurableBeanFactory) beanFactory;
	}
}
