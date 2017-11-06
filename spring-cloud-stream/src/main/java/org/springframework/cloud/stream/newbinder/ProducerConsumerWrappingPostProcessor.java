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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.core.ResolvableType;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 *
 * Implementation of {@link BeanPostProcessor} which converts {@link Consumer} type
 * beans to {@link Function}&lt;SomeType, Void&gt;}
 *
 * @author Oleg Zhurakousky
 *
 */
class ProducerConsumerWrappingPostProcessor implements BeanPostProcessor {

	private final Logger logger = LoggerFactory.getLogger(ProducerConsumerWrappingPostProcessor.class);

	private boolean outputProducingConsumerExists;

	@Autowired
	private DefaultListableBeanFactory beanFactory;

	/**
	 *
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		if (bean instanceof Supplier || bean instanceof Function || bean instanceof Consumer) {
			ResolvableType[] genericParameters = this.discoverArgumetsType(beanName);
			if (bean instanceof Consumer) {
				Consumer consumer = (Consumer) bean;
				bean = (Function)x -> {
					consumer.accept(x);
					return null;
				};
			}
			if (bean instanceof Function) {
				bean = new ConsumerWrapper((Function)bean, genericParameters);
			}
			else {
				bean = new ProducerWrapper((Supplier)bean, genericParameters);
			}
		}
		return bean;
	}

	/**
	 *
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
	 *
	 */
	private void logFunctionRegistration(String name, ResolvableType... signature) {
		if (logger.isDebugEnabled()){
			String mapping = Arrays.asList(signature).toString().replaceAll("\\[", "").replaceAll("]", "");
			String functionalInterfaceName = (signature.length == 2 ? "Function" : "Supplier");
			logger.debug("Registered " + functionalInterfaceName + "; " + (StringUtils.hasText(name) ? name : "") + "(" + mapping + ")");
		}
	}
}
