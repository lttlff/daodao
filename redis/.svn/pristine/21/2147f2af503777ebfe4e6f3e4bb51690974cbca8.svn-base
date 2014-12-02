package org.dacell.redis.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.caucho.hessian.io.HessianInput;
import com.caucho.hessian.io.HessianOutput;

/**
 * @author lff 2013-3-15 下午04:33:14
 * 
 */
public class SerializeUtil {
	
	private final static Log LOG = LogFactory.getLog(SerializeUtil.class);
	

	public static byte[] serialize(Object object) {
		if(object==null){
			return null;
		}
		ByteArrayOutputStream os = null;
		HessianOutput ho = null;
		try {
			os = new ByteArrayOutputStream();  
			ho = new HessianOutput(os);  
			ho.writeObject(object);
			byte[] bytes = os.toByteArray();
			return bytes;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.error(e.getMessage());
		}finally{
			if(ho!=null){
				try {
					ho.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(os !=null){
				try {
					os.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	    return null;
	}
	
	public static byte[] serializeKey(String string) {
		try {
			return string != null ? string.getBytes("UTF-8") : null;
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	} 

	public static byte[][] serializes(Object... values) {
		byte[][] strs = new byte[values.length][];
		for (int i = 0; i < values.length; i++) {
			strs[i] = serialize(values[i]);
		}
		return strs;
	}
	
	public static byte[][] serializeKeys(String... values) {
		byte[][] strs = new byte[values.length][];
		for (int i = 0; i < values.length; i++) {
			strs[i] = serializeKey(values[i]);
		}
		return strs;
	}

	/**
	 * 序列化Map对象，java的主类型，不序列化
	 * 
	 * @param value
	 * @return
	 */
	public static Map<byte[], byte[]> serializeMap(Map<String, Object> map) {
		if (map == null || map.isEmpty()) {
			return null;
		}
		Map<byte[], byte[]> result = new HashMap<byte[], byte[]>();

		for (Iterator<String> it = map.keySet().iterator(); it.hasNext();) {
			String field = it.next();
			Object value = map.get(field);
			
			byte[] byte_field = serialize(field);
			byte[] storeValue = serialize(value);
			result.put(byte_field, storeValue);
		}

		return result;
	}
	
	public static Object unSerialize(byte[] bytes) {
		if(bytes == null){
			return null;
		}
		
		ByteArrayInputStream is = null;  
	    HessianInput hi = null;
		try {
			// 反序列化
			is = new ByteArrayInputStream(bytes); 
			hi = new HessianInput(is);
			Object obj = hi.readObject();
			return obj;
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
		}finally{
			if(hi !=null){
				hi.close();
			}
			if(is !=null){
				try {
					is.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return null;
	}
	public static String unSerializeKey(byte[] bytes) {
		try {
			  return bytes != null ? new String(bytes, "UTF-8") : null;
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	} 
	public static List<Object> unSerializeList(List<byte[]> list) {
		if (list == null | list.isEmpty()) {
			return null;
		}
		List<Object> result = new ArrayList<Object>();
		for (byte[] jsonstr : list) {
			result.add(unSerialize(jsonstr));
		}
		return result;
	}
	
	public static Set<Object> unSerializeSet(Set<byte[]> set) {
		if (set == null | set.isEmpty()) {
			return null;
		}
		Set<Object> result = new HashSet<Object>();
		for (byte[] jsonstr : set) {
			result.add(unSerialize(jsonstr));
		}
		return result;
	}
	public static Set<String> unSerializeKeySet(Set<byte[]> set) {
		if (set == null | set.isEmpty()) {
			return null;
		}
		Set<String> result = new HashSet<String>();
		for (byte[] jsonstr : set) {
			result.add(unSerializeKey(jsonstr));
		}
		return result;
	}
	
	public static Map<String, Object> unSerializeMap(Map<byte[], byte[]> map) {
		if (map == null || map.isEmpty()) {
			return null;
		}
		Map<String, Object> result = new HashMap<String, Object>();

		for (Iterator<byte[]> it = map.keySet().iterator(); it.hasNext();) {
			byte[] byte_field = it.next();
			byte[] value = map.get(byte_field);
			
			String field = (String)unSerialize(byte_field);
			Object storeValue = unSerialize(value);
			result.put(field, storeValue);
		}

		return result;
	}
	
	/**
     * 计算一个对象所占字节数
     * @param o对象，该对象必须继承Serializable接口即可序列化
     * @return
     * @throws IOException
     */
	 public static int size(final Object o) throws IOException {
		  if (o == null) {
			  return 0;
		  }
		  ByteArrayOutputStream buf = new ByteArrayOutputStream(4096);
		  ObjectOutputStream out = new ObjectOutputStream(buf);
		  out.writeObject(o);
		  out.flush();
		  buf.close();
		  
		  return buf.size();
	 }
}
