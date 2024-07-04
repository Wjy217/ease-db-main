/*
 *@Type CommandPos.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:35
 * @version
 */
package model.command;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class CommandPos {
    private int pos;  //命令的起始位置
    private int len;   //命令的长度

    public CommandPos(int pos, int len) {
        this.pos = pos;
        this.len = len;
    }

    @Override
    public String toString() {
        return "CommandPos{" +
                "pos=" + pos +
                ", len=" + len +
                '}';
    }
}
