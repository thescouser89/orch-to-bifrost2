package org.jboss.pnc.migrate;

import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.hibernate.engine.jdbc.BlobProxy;
import org.jboss.pnc.common.concurrent.Sequence;
import org.jboss.pnc.common.pnc.LongBase32IdConverter;
import org.jboss.pnc.migrate.model.FinalLog;
import org.jboss.pnc.migrate.model.LogEntry;

import java.io.FileReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@ApplicationScoped
public class Main {

    @ConfigProperty(name = "csv.folder")
    String pathOfCsv;

    @Scheduled(every = "1m", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void run() throws Exception {

        // Set the node id for the migrator
        Sequence.setNodeId(99);

        List<Path> files = Files
                .list(Path.of(pathOfCsv))
                .filter(p -> p.toFile().isFile())
                .filter(p -> p.toString().endsWith(".csv"))
                .toList();
        for (Path path : files) {

            Reader in = new FileReader(path.toString());
            Log.infof("-- Processing: %s", path);

            Iterable<CSVRecord> records = CSVFormat.POSTGRESQL_CSV.parse(in);

            for (CSVRecord record : records) {

                String[] line = new String[5];
                line[0] = record.get(0);
                line[1] = record.get(1);
                line[2] = record.get(2).equals("f") ? "false" : "true";
                line[3] = record.get(3);
                line[4] = record.get(4);

                processCsvEntry(line);
            }
            Log.infof("-- Done!");
        }
    }

    @Transactional
    public void processCsvEntry(String[] line) throws Exception {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS][.SS][.S]x");

        String id = line[0];
        String endTime = line[1];
        String temporaryBuild = line[2];
        String alignmentLog = line[3];
        String buildLog = line[4];

        LocalDateTime endDateTime = LocalDateTime.parse(endTime, formatter);

        long processContext = LongBase32IdConverter.toLong(id);

        LogEntry toUse;
        Optional<LogEntry> logEntry = LogEntry.findFirstLogEntryWithProcessContext(processContext);

        if (logEntry.isPresent()) {
            toUse = logEntry.get();
        } else {
            Log.infof("Creating new log entry for %s", processContext);
            LogEntry newLogEntry = new LogEntry();
            newLogEntry.id = Sequence.nextId();
            newLogEntry.processContext = processContext;
            newLogEntry.temporary = Boolean.valueOf(temporaryBuild);
            newLogEntry.persist();
            toUse = newLogEntry;
        }

        Collection<FinalLog> alignmentLogs = FinalLog.getFinalLogsWithoutPreviousRetries(processContext, "alignment-log");
        Collection<FinalLog> buildLogs = FinalLog.getFinalLogsWithoutPreviousRetries(processContext, "build-log");

        if (alignmentLogs.isEmpty()) {
            Log.infof("Inserting new alignment log for %s", processContext);
            insertLog(toUse, "org.jboss.pnc._userlog_.alignment-log", endDateTime.atOffset(ZoneOffset.UTC), alignmentLog, "alignment-log");
        } else {
            Log.infof("No alignment log to do");
        }

        if (buildLogs.isEmpty()) {
            Log.infof("Inserting new build log for %s", processContext);
            insertLog(toUse, "org.jboss.pnc._userlog_.build-agent", endDateTime.atOffset(ZoneOffset.UTC), buildLog, "build-log");
        } else {
            Log.infof("No build log to do");
        }
    }

    @Transactional
    public void insertLog(LogEntry logEntry, String loggerName, OffsetDateTime endTimeDate, String log, String tag) {

        if (log == null) {
            log = "";
        }

        String md5sum = DigestUtils.md5Hex(log).toLowerCase();
        int size = log.length();
        FinalLog finalLog = new FinalLog();

        finalLog.id = Sequence.nextId();
        finalLog.logEntry = logEntry;
        finalLog.eventTimestamp = endTimeDate;
        finalLog.loggerName = loggerName;
        finalLog.md5sum = md5sum;
        finalLog.size = size;
        finalLog.tags = Set.of(tag);
        finalLog.logContent = BlobProxy.generateProxy(log.getBytes(StandardCharsets.UTF_8));
        finalLog.persist();
    }
}