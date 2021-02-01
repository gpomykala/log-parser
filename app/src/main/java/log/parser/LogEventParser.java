package log.parser;

import log.parser.model.EventState;
import log.parser.model.LogEvent;
import org.zalando.flatjson.Json;

import java.util.Map;

public class LogEventParser {
    public static LogEvent parseEvent(String jsonString){
        Map<String, Json> json = Json.parse(jsonString).asObject();
        LogEvent evt = new LogEvent();
        evt.id = json.get("id").asString();
        evt.timestamp = json.get("timestamp").asLong();
        evt.state = GetState(json.get("state").asString());
        evt.host = json.get("host").asString();
        evt.type = json.get("type").asString();
        return evt;
    }

    private static EventState GetState(String state) {
        if (state.equals("STARTED")) return  EventState.started;
        else if (state.equals("FINISHED")) return  EventState.finished;
        else return  EventState.none;
    }

}
