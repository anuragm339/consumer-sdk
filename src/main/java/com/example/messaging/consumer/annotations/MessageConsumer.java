package com.example.messaging.consumer.annotations;

import io.micronaut.context.annotation.AliasFor;
import jakarta.inject.Singleton;
import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Singleton
public @interface MessageConsumer {
    String group() default "";
}
