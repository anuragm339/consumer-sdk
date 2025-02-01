package com.example.messaging.consumer.registry;

import com.example.messaging.consumer.annotations.MessageConsumer;
import com.example.messaging.consumer.annotations.MessageHandler;
import com.example.messaging.consumer.config.MessageConsumerProperties;
import com.example.messaging.consumer.connection.ConnectionManager;
import com.example.messaging.consumer.rsocket.impl.ConsumerRSocketFactory;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.context.event.BeanCreatedEventListener;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class MessageConsumerBeanPostProcessor implements BeanCreatedEventListener<Object> {
    private static final Logger logger = LoggerFactory.getLogger(MessageConsumerBeanPostProcessor.class);

    @Context
    private final ApplicationContext applicationContext;

    @Inject
    public MessageConsumerBeanPostProcessor(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    public Object onCreated(BeanCreatedEvent<Object> event) {
        Object bean = event.getBean();
        Class<?> beanClass = bean.getClass();

        if (beanClass.isAnnotationPresent(MessageConsumer.class)) {
            MessageConsumerProperties properties = applicationContext.getBean(MessageConsumerProperties.class);
            ConsumerRSocketFactory rSocketFactory = applicationContext.getBean(ConsumerRSocketFactory.class);
            processConsumerBean(bean, beanClass, properties, rSocketFactory);
        }

        return bean;
    }

    private void processConsumerBean(Object bean, Class<?> beanClass,
                                     MessageConsumerProperties properties, ConsumerRSocketFactory rSocketFactory) {
        try {
            MessageConsumer annotation = beanClass.getAnnotation(MessageConsumer.class);
            String groupId = annotation.group().isEmpty() ? properties.getDefaultGroupId() : annotation.group();
            String consumerId = beanClass.getName();

            Map<String, Method> handlers = findMessageHandlers(beanClass);
            DefaultMessageHandler messageHandler = new DefaultMessageHandler(bean, handlers);

            ConnectionManager connectionManager = new ConnectionManager(
                    consumerId,
                    groupId,
                    messageHandler,
                    rSocketFactory,
                    properties
            );

            connectionManager.getConnection().subscribe();
            logger.info("Initialized consumer: {}, group: {}", consumerId, groupId);
        } catch (Exception e) {
            logger.error("Failed to process consumer bean: {}", beanClass.getName(), e);
            throw new RuntimeException("Failed to process consumer bean", e);
        }
    }
    // ... rest of the code remains the same



    private Map<String, Method> findMessageHandlers(Class<?> beanClass) {
        Map<String, Method> handlers = new ConcurrentHashMap<>();
        for (Method method : beanClass.getDeclaredMethods()) {
            MessageHandler annotation = method.getAnnotation(MessageHandler.class);
            if (annotation != null) {
                handlers.put(annotation.value(), method);
                logger.debug("Found handler method: {} for type: {}", method.getName(), annotation.value());
            }
        }
        return handlers;
    }
}
