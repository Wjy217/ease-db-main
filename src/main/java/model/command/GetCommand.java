package model.command;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class GetCommand extends AbstractCommand{
    private String key;

    public GetCommand(String key) {
        super(CommandTypeEnum.GET);
        this.key = key;
    }
}
