package Spark;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;

import java.util.Iterator;
import java.util.Map;

/**
 * @author Malegod_xiaofei
 * @create 2023-11-30-15:28
 */
public class Test {
    public static void main(String[] args) {
        String s = "{\"品牌\":\"其他\"}";
        Map baseInfo = JSONObject.parseObject(s, Feature.OrderedField);
        Iterator<Map.Entry<String, String>> iter = baseInfo.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            System.out.println(entry.getKey());
            System.out.println(entry.getValue());
        }

    }
}
