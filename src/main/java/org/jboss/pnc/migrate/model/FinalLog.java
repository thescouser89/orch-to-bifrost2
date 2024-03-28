package org.jboss.pnc.migrate.model;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import io.quarkus.panache.common.Sort;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.ManyToOne;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Entity
public class FinalLog extends PanacheEntityBase {

    @Id
    public long id;

    @ManyToOne(optional = false)
    public LogEntry logEntry;

    @Column(nullable = false)
    public OffsetDateTime eventTimestamp;

    @Column(nullable = false)
    public String loggerName;

    @Column(length = 32, nullable = false)
    public String md5sum;

    @ElementCollection
    public Set<String> tags;

    @Lob
    public Blob logContent;

    public long size;

    public static void copyFinalLogsToOutputStream(long processContext, String tag, OutputStream outputStream)
            throws SQLException, IOException {
        Collection<FinalLog> logs = getFinalLogsWithoutPreviousRetries(processContext, tag);

        // write all those logs to the output stream now.
        for (FinalLog finalLog : logs) {
            finalLog.logContent.getBinaryStream().transferTo(outputStream);
        }
    }

    /**
     * Get all the FinalLog objects for a particular process context and tag If multiple entries exist for a particular
     * process context, tag, and loggerName, the last entry for that loggerName is picked up. The multiple entries
     * happen when retries happen for a component, and we only want to show the last attempt in the logs.
     *
     * @param processContext process context to find
     * @param tag tag of the final log
     * @return collection of final log objects
     */
    public static Collection<FinalLog> getFinalLogsWithoutPreviousRetries(long processContext, String tag) {
        List<LogEntry> logEntries = LogEntry.list("processContext", processContext);

        // find all logs for this process context
        List<FinalLog> finalLogs = list("logEntry in ?1", Sort.by("eventTimestamp"), logEntries);

        // narrow it down to the specific tag
        finalLogs = finalLogs.stream().filter(a -> a.tags.contains(tag)).collect(Collectors.toList());

        // use LinkedHashMap to preserve order of insertion
        LinkedHashMap<String, FinalLog> logMap = new LinkedHashMap<>();

        // iterate through the finalLogs in order of eventTimestamp, and only keep the last final log for a loggername,
        // to get rid of previous retries final logs
        for (FinalLog finalLog : finalLogs) {
            logMap.put(finalLog.loggerName, finalLog);
        }
        return logMap.values();
    }
}
