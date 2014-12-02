package org.dacell.redis.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RedisCacheAble {

	String key();

	/**
	 * 目前暂未实现
	 * 
	 * @return
	 */
	String condition() default "";

	/**
	 * -1表示不过期(seconds)
	 * 
	 * @return
	 */
	int expiration() default -1;
}
