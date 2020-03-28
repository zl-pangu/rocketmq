package com.example.demo.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * @Author: zhouliang
 * @Date: 2018/7/4 16:29
 */
@Slf4j
public class JsonUtil {
    private static ObjectMapper mapper=new ObjectMapper();

    static {
        //反序列化json的时候禁用未知属性打断反序列化的功能
        //列如：json里面有十个属性，我们的bean里面只有两个，其他将被忽略
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        //FAIL_ON_EMPTY_BEANS决定了在没有找到类型的存取器时发生了什么（并且没有注释表明它是被序列化的）。如果启用（默认），
        // 将抛出一个异常来指明这些是非序列化类型;如果禁用了，它们将被序列化为空对象，即没有任何属性。
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS,false);
        //在序列化时，只有那些值为null或被认为为空的值的属性才不会被包含在内。
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
    }

    /**
     * 对象转json字符串
     * @param obj
     * @param <T>
     * @return
     */
    public static <T>String objectToJson(T obj){
        if(obj == null){
            return null;
        }
        try {
            return obj instanceof String ? (String) obj : mapper.writeValueAsString(obj);
        } catch (Exception e) {
            log.warn("Parse Object to Json error",e);
            log.error(e.getMessage());
            return null;
        }
    }

    /**
     * 对象转格式化的json字符串
     * @param obj
     * @param <T>
     * @return
     */
    public static <T>String objectToJsonPretty(T obj){
        if(obj == null){
            return null;
        }
        try {
            return obj instanceof String ? (String) obj : mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
        } catch (Exception e) {
            log.warn("Parse Object to Json error",e);
            log.error(e.getMessage());
            return null;
        }
    }

    /**
     * json转java对象
     * @param src
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T>T jsonToObject(String src,Class<T> clazz){
        if(StringUtils.isEmpty(src) || clazz == null){
            return null;
        }
        try {
            return clazz.equals(String.class) ? (T) src : mapper.readValue(src,clazz);
        } catch (Exception e) {
            log.warn("Parse Json to Object error",e);
            log.error(e.getMessage());
            return null;
        }
    }

    /**
     * json转java对象或list、map
     * @param src
     * @param typeReference
     * @param <T>
     * @return
     */
    public static <T>T jsonToObject(String src, TypeReference<T> typeReference){
        if(StringUtils.isEmpty(src) || typeReference == null){
            return null;
        }
        try {
            return (T)(typeReference.getType().equals(String.class) ? src : mapper.readValue(src, typeReference));
        } catch (Exception e) {
            log.warn("Parse Json to Object error",e);
            log.error(e.getMessage());
            return null;
        }
    }

    /**
     * json转java对象或list、map
     * @param src
     * @param collectionClass
     * @param elementClasses
     * @param <T>
     * @return
     */
    public static <T>T jsonToObject(String src, Class<?> collectionClass,Class<?>... elementClasses){
        JavaType javaType = mapper.getTypeFactory().constructParametricType(collectionClass,elementClasses);
        try {
            return mapper.readValue(src,javaType);
        } catch (Exception e) {
            log.warn("Parse Json to Object error",e);
            log.error(e.getMessage());
            return null;
        }
    }


}
