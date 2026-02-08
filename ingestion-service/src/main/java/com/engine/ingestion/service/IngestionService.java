package com.engine.ingestion.service;

import com.engine.ingestion.repository.TransactionRepository;
import com.engine.ingestion.repository.ActivityLogRepository;
import com.engine.ingestion.entity.Transaction;
import com.engine.ingestion.entity.ActivityLog;
import com.engine.ingestion.grpc.TransactionDto;
import com.engine.ingestion.grpc.ActivityDto;
import com.engine.ingestion.kafka.IngestionEventPublisher;
import org.springframework.stereotype.Service;

@Service
public class IngestionService {
    private final TransactionRepository transactionRepo;
    private final ActivityLogRepository activityRepo;
    private final IngestionEventPublisher eventPublisher;

    public IngestionService(
            TransactionRepository transactionRepo,
            ActivityLogRepository activityRepo,
            IngestionEventPublisher eventPublisher
    ) {
        this.transactionRepo = transactionRepo;
        this.activityRepo = activityRepo;
        this.eventPublisher = eventPublisher;
    }

    public void saveTransaction(TransactionDto dto) {
        Transaction tx = new Transaction();
        tx.setEventId(dto.getId());
        tx.setDescription(dto.getDescription());
        tx.setAmount(dto.getAmount());
        tx.setCurrency(dto.getCurrency());
        tx.setStatus(dto.getStatus());
        tx.setTimestamp(dto.getTimestamp());
        transactionRepo.save(tx);
        eventPublisher.publishTransaction(dto);
    }

    public void saveActivity(ActivityDto dto) {
        ActivityLog log = new ActivityLog();
        log.setEventId(dto.getId());
        log.setActivity(dto.getActivity());
        log.setDescription(dto.getDescription());
        log.setTimestamp(dto.getTimestamp());
        activityRepo.save(log);
        eventPublisher.publishActivity(dto);
    }
}
