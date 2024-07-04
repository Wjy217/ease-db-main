/*
 *@Type ConvertUtil.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:09
 * @version
 */
package utils;

import com.alibaba.fastjson.JSONObject;
import model.command.*;

//从JSON数据中解析出来的命令信息转换为具体的命令对象，以便后续进行命令的处理和执行
public class CommandUtil {
    public static final String TYPE = "type";

    public static Command jsonToCommand(JSONObject value){
        if (value.getString(TYPE).equals(CommandTypeEnum.SET.name())) {
            return value.toJavaObject(SetCommand.class);
        } else if (value.getString(TYPE).equals(CommandTypeEnum.RM.name())) {
            return value.toJavaObject(RmCommand.class);
        }else if (value.getString(TYPE).equals(CommandTypeEnum.GET.name())){
            return value.toJavaObject(GetCommand.class);
        }
        return null;
    }
}
