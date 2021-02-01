package log.parser;

import log.parser.model.EventState;
import log.parser.model.LogEvent;
import log.parser.model.LogEventDetails;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class LogEventProcessor {
    private final Logger logger;
    // TODO optimize initial size and load factor for usual log file size
    Map<String, LogEvent> eventStash = new HashMap<>();
    // TODO should be adjusted for node's memory size
    final int MAX_STASH_SIZE = 1 << 30;
    int eventStashSize = 0;
    LogEventWriter eventWriter;

    public LogEventProcessor(LogEventWriter eventWriter, Logger logger) {
        this.eventWriter = eventWriter;
        this.logger = logger;
    }

    public void Process(LogEvent evt) {
        if (evt.state == EventState.none) {
            logger.warn("Event state not determined, skipping", evt);
            return;
        }
        else if (evt.state == EventState.started) {
            if(eventStashSize == MAX_STASH_SIZE) {
                logger.error(eventStashSize + " unmatched events in stash, new events will not be processed");
                // TODO evict oldest entries / clear stash / break execution ?
                return;
            }
            eventStash.put(evt.id, evt);
            eventStashSize++;
        }
        else {
            LogEvent match = eventStash.get(evt.id);
            if(match == null) {
                logger.error(evt.id + " does not have matching STARTED entry, skipping...", evt);
                return;
            }

            OnFinishedEvent(evt, evt.timestamp - match.timestamp);
            eventStash.remove(evt.id);
        }
    }

    void OnFinishedEvent(LogEvent evt, long duration) {
        LogEventDetails evtDetails = LogEventDetails.FromEvent(evt, duration);
        eventWriter.write(evtDetails);
    }
}
