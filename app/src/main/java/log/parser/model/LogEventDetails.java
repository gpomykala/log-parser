package log.parser.model;

public class LogEventDetails {
    public String id;
    public long duration;
    public String type;
    public String host;
    public boolean alert;
    final static  int ALERT_THRESHOLD = 4;
    public static LogEventDetails FromEvent(LogEvent evt, long duration) {
        LogEventDetails details = new LogEventDetails();
        details.id = evt.id;
        details.duration = duration;
        details.type = evt.type;
        details.host = evt.host;
        if (duration > ALERT_THRESHOLD) {
            details.alert = true;
        }
        return details;
    }
}
