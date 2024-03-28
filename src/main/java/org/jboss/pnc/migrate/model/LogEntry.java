package org.jboss.pnc.migrate.model;


import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import io.quarkus.panache.common.Sort;
import io.smallrye.common.constraint.NotNull;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

import java.util.Optional;

@Entity
public class LogEntry extends PanacheEntityBase {

    @Id
    public long id;

    @NotNull
    @Column(nullable = false)
    public Long processContext;

    @Column(length = 10)
    public String processContextVariant;

    @Column(length = 32)
    public String requestContext;

    public Boolean temporary;

    public Long buildId;

    public static Optional<LogEntry> findFirstLogEntryWithProcessContext(long processContext) {
        return find("processContext = ?1", Sort.by("id").ascending(), processContext).firstResultOptional();
    }
}
