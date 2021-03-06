package org.dacell.redis.asspect;

import java.lang.reflect.Method;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.dacell.redis.annotation.DelRedisCache;
import org.dacell.redis.annotation.RedisCacheAble;
import org.dacell.redis.servicemanager.RedisServiceHandler;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;

/**
 * redis 注解切面处理
 * @author Administrator
 *
 */
@Aspect
@Component
public class RedisCacheAspect {
	private static Log logger = LogFactory.getLog(RedisCacheAspect.class);
//	private RedisManager redisManager = RedisManager.getInstance();
	private RedisServiceHandler redisServiceHandler = RedisServiceHandler.getInstance();
	/**
	 * 删除缓存 key 支持通配符
	 *@author lff 2013-5-28 下午03:57:29  
	 * @param jp
	 */
	@After(value = "@annotation(org.dacell.redis.annotation.DelRedisCache)")
	public void deleteCache(JoinPoint jp) {
		Signature signature = jp.getSignature();
		MethodSignature methodSignature = (MethodSignature) signature;
		String[] parameterNames = methodSignature.getParameterNames();
		Method method = methodSignature.getMethod();
		DelRedisCache delRedisCache = method
				.getAnnotation(DelRedisCache.class);
		String key = delRedisCache.key();
		if (!StringUtils.isEmpty(key)) {
			Object[] args = jp.getArgs();
			if (args != null && parameterNames != null) {
				ExpressionParser  parser = new SpelExpressionParser();  
				EvaluationContext context = new StandardEvaluationContext();
				for (int i = 0; i < args.length && i < parameterNames.length; i++) {
					Object arg = args[i];
					String parameterName = parameterNames[i];
//					key = key.replaceAll("\\{" + parameterName + "\\}",
//							arg+"");
					context.setVariable(parameterName, arg);
				}
				Expression expression =  parser.parseExpression(key);  
				key = (String)expression.getValue(context);
			}
		}else{
			key = method.getName();
			Object[] args = jp.getArgs();
			if (args != null && parameterNames != null) {
				for (int i = 0; i < args.length && i < parameterNames.length; i++) {
					Object arg = args[i];
					key += "_"+	arg;
				}
			}
		}
		logger.debug("delete cache  key='" + key + "'");
		try {
			redisServiceHandler.deleteMatchKey(key);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("error in delete cache  key='" + key + "'");
		}
	}
	/**
	 * 添加缓存
	 *@author lff 2013-5-28 下午03:57:55  
	 * @param jp
	 * @return
	 * @throws Throwable
	 */
	@Around(value = "@annotation(org.dacell.redis.annotation.RedisCacheAble)")
	public Object addCache(ProceedingJoinPoint jp) throws Throwable {
		Signature signature = jp.getSignature();
		MethodSignature methodSignature = (MethodSignature) signature;
		String[] parameterNames = methodSignature.getParameterNames();
		Method method = methodSignature.getMethod();

		RedisCacheAble redisCacheAble = method
				.getAnnotation(RedisCacheAble.class);
		String key = redisCacheAble.key();
		int expireTime = redisCacheAble.expiration();
		Object result = null;
		if (!StringUtils.isEmpty(key)) {
			Object[] args = jp.getArgs();
			if (args != null && parameterNames != null) {
				ExpressionParser  parser = new SpelExpressionParser();  
				EvaluationContext context = new StandardEvaluationContext();
				for (int i = 0; i < args.length && i < parameterNames.length; i++) {
					Object arg = args[i];
					String parameterName = parameterNames[i];
//					key = key.replaceAll("\\{" + parameterName + "\\}",
//							arg+"");
					context.setVariable(parameterName, arg);
				}
				Expression expression =  parser.parseExpression(key);  
				key = (String)expression.getValue(context);
			}
		}else{
			key = method.getName();
			Object[] args = jp.getArgs();
			if (args != null && parameterNames != null) {
				for (int i = 0; i < args.length && i < parameterNames.length; i++) {
					Object arg = args[i];
					key += "_"+	arg;
				}
			}
		}
		logger.debug("get cache   key='" + key + "'");
		try {
			result = redisServiceHandler.get(key);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("error in get cache  key='" + key + "'");
		}
		if (result == null) {
			result = jp.proceed();
			if (result != null) {
				logger.debug("set cache  value='" + result + "'");
				try {
					redisServiceHandler.set(key, result);
					if(expireTime>-1){
						redisServiceHandler.expire(key, expireTime);
					}
				} catch (Exception e) {
					e.printStackTrace();
					logger.error("error in set cache  key='" + key + "'");
				}
			}
		}
		return result;
	}

	public static void main2(String[] args) {
		String abc = "asdfasdf{name}";
		abc = abc.replaceAll("\\{name\\}", "中文");
		System.out.println(abc);
	}

}
