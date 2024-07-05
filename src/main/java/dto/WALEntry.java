package dto;

import lombok.Data;

@Data
public class WALEntry {
    private String commandType;
    private String key;
    private String value;

    public WALEntry(String commandType, String key, String value) {
        this.commandType = commandType;
        this.key = key;
        this.value = value;
    }

}
