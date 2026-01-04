package org.example.consent.repository;

import org.example.consent.model.ConsentRule;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;

public interface ConsentRuleRepository extends JpaRepository<ConsentRule, Long> {
    Optional<ConsentRule> findByAthleteIdAndStatus(String athleteId, String status);
}
