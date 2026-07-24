use std::env;

const DEFAULT_MAX_DELIVERY_ATTEMPTS: u32 = 3;

#[derive(Debug)]
struct OrchestratorConfig {
    project_name: String,
    control_plane_role: String,
    scheduler_mode: String,
    local_capacity_slots: u32,
    remote_wake_threshold_slots: u32,
    idle_powerdown_after_seconds: u32,
    kafka_bootstrap_servers: String,
    localstack_endpoint: String,
    paperclip_endpoint: String,
    max_in_flight: u32,
    retry_limit: u32,
}

impl OrchestratorConfig {
    fn from_env() -> Self {
        Self {
            project_name: read_env("ATHERNEX_PROJECT_NAME", "Project Athernex"),
            control_plane_role: read_env("ATHERNEX_CONTROL_PLANE_ROLE", "local-control-plane"),
            scheduler_mode: read_env("ATHERNEX_SCHEDULER_MODE", "contract-only"),
            local_capacity_slots: read_u32_env("ATHERNEX_LOCAL_CAPACITY_SLOTS", 2),
            remote_wake_threshold_slots: read_u32_env("ATHERNEX_REMOTE_WAKE_THRESHOLD_SLOTS", 3),
            idle_powerdown_after_seconds: read_u32_env(
                "ATHERNEX_IDLE_POWERDOWN_AFTER_SECONDS",
                900,
            ),
            kafka_bootstrap_servers: read_env("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
            localstack_endpoint: read_env("LOCALSTACK_ENDPOINT", "http://127.0.0.1:4566"),
            paperclip_endpoint: read_env("PAPERCLIP_ENDPOINT", "http://127.0.0.1:3100"),
            max_in_flight: read_u32_env("AGENT_MAX_IN_FLIGHT", 8),
            retry_limit: read_u32_env("AGENT_RETRY_LIMIT", 3),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum WorkflowState {
    Received,
    Validated,
    Scheduled,
    Running,
    Reviewing,
    Accepted,
    Retrying,
    Quarantined,
    Deadlettered,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KafkaTopic {
    AgentCommands,
    AgentObservations,
    AgentResults,
    AgentReview,
    AgentDeadletter,
    SchedulerCapacity,
    PowerCommands,
    PowerObservations,
    SecurityFindings,
    AgentAudit,
}

impl KafkaTopic {
    fn as_str(self) -> &'static str {
        match self {
            KafkaTopic::AgentCommands => "agent.commands",
            KafkaTopic::AgentObservations => "agent.observations",
            KafkaTopic::AgentResults => "agent.results",
            KafkaTopic::AgentReview => "agent.review",
            KafkaTopic::AgentDeadletter => "agent.deadletter",
            KafkaTopic::SchedulerCapacity => "scheduler.capacity",
            KafkaTopic::PowerCommands => "power.commands",
            KafkaTopic::PowerObservations => "power.observations",
            KafkaTopic::SecurityFindings => "security.findings",
            KafkaTopic::AgentAudit => "agent.audit",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MessageEnvelope {
    topic: KafkaTopic,
    idempotency_key: String,
    correlation_id: String,
    workflow_state: WorkflowState,
    attempt: u32,
    max_attempts: u32,
    payload: String,
}

impl MessageEnvelope {
    fn new(
        topic: KafkaTopic,
        run_id: &str,
        workflow_state: WorkflowState,
        payload: String,
        config: &OrchestratorConfig,
    ) -> Self {
        Self {
            topic,
            idempotency_key: format!("{run_id}:{}", topic.as_str()),
            correlation_id: run_id.to_string(),
            workflow_state,
            attempt: 1,
            max_attempts: config.retry_limit.max(DEFAULT_MAX_DELIVERY_ATTEMPTS),
            payload,
        }
    }

    fn with_delivery_failure(&self, topic: KafkaTopic, reason: &str) -> Self {
        Self {
            topic,
            idempotency_key: format!("{}:{}", self.correlation_id, topic.as_str()),
            correlation_id: self.correlation_id.clone(),
            workflow_state: WorkflowState::Deadlettered,
            attempt: self.attempt,
            max_attempts: self.max_attempts,
            payload: format!(
                "delivery_failed source_topic={} reason={} payload={}",
                self.topic.as_str(),
                reason,
                self.payload
            ),
        }
    }
}

trait MessageProducer {
    fn publish(&mut self, envelope: MessageEnvelope);
}

trait MessageConsumer {
    fn drain_topic(&mut self, topic: KafkaTopic) -> Vec<MessageEnvelope>;
}

#[derive(Debug, Default)]
struct InMemoryBroker {
    envelopes: Vec<MessageEnvelope>,
}

impl MessageProducer for InMemoryBroker {
    fn publish(&mut self, envelope: MessageEnvelope) {
        self.envelopes.push(envelope);
    }
}

impl MessageConsumer for InMemoryBroker {
    fn drain_topic(&mut self, topic: KafkaTopic) -> Vec<MessageEnvelope> {
        let mut matched = Vec::new();
        let mut remaining = Vec::new();

        for envelope in self.envelopes.drain(..) {
            if envelope.topic == topic {
                matched.push(envelope);
            } else {
                remaining.push(envelope);
            }
        }

        self.envelopes = remaining;
        matched
    }
}

#[derive(Debug, PartialEq, Eq)]
enum DeliveryFailure {
    Transient(&'static str),
    Permanent(&'static str),
}

#[derive(Debug)]
struct JobRequest {
    run_id: &'static str,
    required_slots: u32,
    priority: JobPriority,
    allows_remote_capacity: bool,
}

#[derive(Debug)]
enum JobPriority {
    Interactive,
    Batch,
    Maintenance,
}

#[derive(Debug)]
struct CapacitySnapshot {
    local_available_slots: u32,
    remote_online_slots: u32,
    remote_powering_slots: u32,
    idle_seconds: u32,
}

#[derive(Debug, PartialEq, Eq)]
enum SchedulingDecision {
    RunHere {
        run_id: &'static str,
        slots: u32,
    },
    DispatchRemote {
        run_id: &'static str,
        slots: u32,
    },
    RequestPowerOn {
        run_id: &'static str,
        target_group: &'static str,
        slots: u32,
    },
    Hold {
        run_id: &'static str,
        reason: &'static str,
    },
    RequestPowerOff {
        target_group: &'static str,
        idle_seconds: u32,
    },
}

fn main() {
    let config = OrchestratorConfig::from_env();

    println!("athernex orchestrator preparation mode");
    println!("project_name={}", config.project_name);
    println!("control_plane_role={}", config.control_plane_role);
    println!("scheduler_mode={}", config.scheduler_mode);
    println!("local_capacity_slots={}", config.local_capacity_slots);
    println!(
        "remote_wake_threshold_slots={}",
        config.remote_wake_threshold_slots
    );
    println!(
        "idle_powerdown_after_seconds={}",
        config.idle_powerdown_after_seconds
    );
    println!("kafka_bootstrap_servers={}", config.kafka_bootstrap_servers);
    println!("localstack_endpoint={}", config.localstack_endpoint);
    println!("paperclip_endpoint={}", config.paperclip_endpoint);
    println!("max_in_flight={}", config.max_in_flight);
    println!("retry_limit={}", config.retry_limit);
    println!("workflow_states={:?}", workflow_states());
    println!("kafka_topics={:?}", kafka_topics());
    println!("sample_kafka_contract={:?}", sample_kafka_contract(&config));
    for decision in sample_scheduling_decisions(&config) {
        println!(
            "sample_scheduling_decision={}",
            describe_decision(&decision)
        );
    }

    if let Some(decision) = decide_idle_action(
        &CapacitySnapshot {
            local_available_slots: config.local_capacity_slots,
            remote_online_slots: 4,
            remote_powering_slots: 0,
            idle_seconds: config.idle_powerdown_after_seconds + 1,
        },
        &config,
    ) {
        println!("sample_idle_decision={}", describe_decision(&decision));
    }
}

fn sample_kafka_contract(config: &OrchestratorConfig) -> Vec<MessageEnvelope> {
    let mut broker = InMemoryBroker::default();
    let power_command = publish_scheduling_observation(
        &mut broker,
        &JobRequest {
            run_id: "batch-remote-001",
            required_slots: 4,
            priority: JobPriority::Batch,
            allows_remote_capacity: true,
        },
        &CapacitySnapshot {
            local_available_slots: config.local_capacity_slots,
            remote_online_slots: 0,
            remote_powering_slots: 0,
            idle_seconds: 0,
        },
        config,
    );
    let mut exhausted_command = power_command.clone();
    exhausted_command.attempt = exhausted_command.max_attempts;
    broker.publish(next_delivery_attempt(
        &exhausted_command,
        DeliveryFailure::Transient("sample broker timeout"),
    ));
    broker.publish(next_delivery_attempt(
        &power_command,
        DeliveryFailure::Permanent("sample invalid envelope"),
    ));

    let mut sample = broker.drain_topic(KafkaTopic::PowerCommands);
    sample.extend(broker.drain_topic(KafkaTopic::AgentDeadletter));
    sample
}

fn sample_scheduling_decisions(config: &OrchestratorConfig) -> [SchedulingDecision; 3] {
    [
        decide_capacity_action(
            &JobRequest {
                run_id: "interactive-local-001",
                required_slots: 1,
                priority: JobPriority::Interactive,
                allows_remote_capacity: false,
            },
            &CapacitySnapshot {
                local_available_slots: config.local_capacity_slots,
                remote_online_slots: 0,
                remote_powering_slots: 0,
                idle_seconds: 0,
            },
            config,
        ),
        decide_capacity_action(
            &JobRequest {
                run_id: "batch-remote-001",
                required_slots: 4,
                priority: JobPriority::Batch,
                allows_remote_capacity: true,
            },
            &CapacitySnapshot {
                local_available_slots: config.local_capacity_slots,
                remote_online_slots: 0,
                remote_powering_slots: 0,
                idle_seconds: 0,
            },
            config,
        ),
        decide_capacity_action(
            &JobRequest {
                run_id: "maintenance-001",
                required_slots: 1,
                priority: JobPriority::Maintenance,
                allows_remote_capacity: false,
            },
            &CapacitySnapshot {
                local_available_slots: config.local_capacity_slots,
                remote_online_slots: 0,
                remote_powering_slots: 0,
                idle_seconds: 0,
            },
            config,
        ),
    ]
}

fn workflow_states() -> [WorkflowState; 9] {
    [
        WorkflowState::Received,
        WorkflowState::Validated,
        WorkflowState::Scheduled,
        WorkflowState::Running,
        WorkflowState::Reviewing,
        WorkflowState::Accepted,
        WorkflowState::Retrying,
        WorkflowState::Quarantined,
        WorkflowState::Deadlettered,
    ]
}

fn kafka_topics() -> [&'static str; 10] {
    [
        KafkaTopic::AgentCommands.as_str(),
        KafkaTopic::AgentObservations.as_str(),
        KafkaTopic::AgentResults.as_str(),
        KafkaTopic::AgentReview.as_str(),
        KafkaTopic::AgentDeadletter.as_str(),
        KafkaTopic::SchedulerCapacity.as_str(),
        KafkaTopic::PowerCommands.as_str(),
        KafkaTopic::PowerObservations.as_str(),
        KafkaTopic::SecurityFindings.as_str(),
        KafkaTopic::AgentAudit.as_str(),
    ]
}

fn publish_scheduling_observation(
    producer: &mut impl MessageProducer,
    job: &JobRequest,
    capacity: &CapacitySnapshot,
    config: &OrchestratorConfig,
) -> MessageEnvelope {
    let decision = decide_capacity_action(job, capacity, config);
    let envelope = envelope_for_decision(job.run_id, &decision, config);

    producer.publish(envelope.clone());
    envelope
}

fn envelope_for_decision(
    run_id: &str,
    decision: &SchedulingDecision,
    config: &OrchestratorConfig,
) -> MessageEnvelope {
    let topic = match decision {
        SchedulingDecision::RunHere { .. } | SchedulingDecision::DispatchRemote { .. } => {
            KafkaTopic::AgentCommands
        }
        SchedulingDecision::RequestPowerOn { .. } | SchedulingDecision::RequestPowerOff { .. } => {
            KafkaTopic::PowerCommands
        }
        SchedulingDecision::Hold { .. } => KafkaTopic::SchedulerCapacity,
    };

    MessageEnvelope::new(
        topic,
        run_id,
        WorkflowState::Scheduled,
        describe_decision(decision),
        config,
    )
}

fn next_delivery_attempt(envelope: &MessageEnvelope, failure: DeliveryFailure) -> MessageEnvelope {
    match failure {
        DeliveryFailure::Permanent(reason) => {
            envelope.with_delivery_failure(KafkaTopic::AgentDeadletter, reason)
        }
        DeliveryFailure::Transient(reason) if envelope.attempt >= envelope.max_attempts => {
            envelope.with_delivery_failure(KafkaTopic::AgentDeadletter, reason)
        }
        DeliveryFailure::Transient(reason) => {
            let mut retry = envelope.clone();
            retry.workflow_state = WorkflowState::Retrying;
            retry.attempt += 1;
            retry.payload = format!(
                "retry source_topic={} reason={} payload={}",
                envelope.topic.as_str(),
                reason,
                envelope.payload
            );
            retry
        }
    }
}

fn decide_capacity_action(
    job: &JobRequest,
    capacity: &CapacitySnapshot,
    config: &OrchestratorConfig,
) -> SchedulingDecision {
    match job.priority {
        JobPriority::Interactive if job.required_slots <= capacity.local_available_slots => {
            return SchedulingDecision::RunHere {
                run_id: job.run_id,
                slots: job.required_slots,
            };
        }
        JobPriority::Maintenance => {
            return SchedulingDecision::Hold {
                run_id: job.run_id,
                reason: "maintenance jobs require an explicit promotion window",
            };
        }
        _ => {}
    }

    if job.required_slots <= capacity.local_available_slots {
        return SchedulingDecision::RunHere {
            run_id: job.run_id,
            slots: job.required_slots,
        };
    }

    if !job.allows_remote_capacity {
        return SchedulingDecision::Hold {
            run_id: job.run_id,
            reason: "job does not allow remote worker capacity",
        };
    }

    if job.required_slots <= capacity.remote_online_slots {
        return SchedulingDecision::DispatchRemote {
            run_id: job.run_id,
            slots: job.required_slots,
        };
    }

    if capacity.remote_powering_slots > 0 {
        return SchedulingDecision::Hold {
            run_id: job.run_id,
            reason: "remote worker group is already powering on",
        };
    }

    if job.required_slots >= config.remote_wake_threshold_slots {
        return SchedulingDecision::RequestPowerOn {
            run_id: job.run_id,
            target_group: "worker-group-a",
            slots: job.required_slots,
        };
    }

    SchedulingDecision::Hold {
        run_id: job.run_id,
        reason: "below remote wake threshold",
    }
}

fn decide_idle_action(
    capacity: &CapacitySnapshot,
    config: &OrchestratorConfig,
) -> Option<SchedulingDecision> {
    if capacity.remote_online_slots > 0
        && capacity.idle_seconds >= config.idle_powerdown_after_seconds
    {
        return Some(SchedulingDecision::RequestPowerOff {
            target_group: "worker-group-a",
            idle_seconds: capacity.idle_seconds,
        });
    }

    None
}

fn describe_decision(decision: &SchedulingDecision) -> String {
    match decision {
        SchedulingDecision::RunHere { run_id, slots } => {
            format!("run_here run_id={run_id} slots={slots}")
        }
        SchedulingDecision::DispatchRemote { run_id, slots } => {
            format!("dispatch_remote run_id={run_id} slots={slots}")
        }
        SchedulingDecision::RequestPowerOn {
            run_id,
            target_group,
            slots,
        } => format!("request_power_on run_id={run_id} target_group={target_group} slots={slots}"),
        SchedulingDecision::Hold { run_id, reason } => {
            format!("hold run_id={run_id} reason={reason}")
        }
        SchedulingDecision::RequestPowerOff {
            target_group,
            idle_seconds,
        } => format!("request_power_off target_group={target_group} idle_seconds={idle_seconds}"),
    }
}

fn read_env(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn read_u32_env(key: &str, default: u32) -> u32 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> OrchestratorConfig {
        OrchestratorConfig {
            project_name: "Project Athernex".to_string(),
            control_plane_role: "local-control-plane".to_string(),
            scheduler_mode: "contract-only".to_string(),
            local_capacity_slots: 2,
            remote_wake_threshold_slots: 3,
            idle_powerdown_after_seconds: 900,
            kafka_bootstrap_servers: "127.0.0.1:9092".to_string(),
            localstack_endpoint: "http://127.0.0.1:4566".to_string(),
            paperclip_endpoint: "http://127.0.0.1:3100".to_string(),
            max_in_flight: 8,
            retry_limit: 3,
        }
    }

    #[test]
    fn interactive_jobs_prefer_available_local_capacity() {
        let decision = decide_capacity_action(
            &JobRequest {
                run_id: "interactive-local-001",
                required_slots: 1,
                priority: JobPriority::Interactive,
                allows_remote_capacity: false,
            },
            &CapacitySnapshot {
                local_available_slots: 2,
                remote_online_slots: 4,
                remote_powering_slots: 0,
                idle_seconds: 0,
            },
            &config(),
        );

        assert_eq!(
            decision,
            SchedulingDecision::RunHere {
                run_id: "interactive-local-001",
                slots: 1
            }
        );
    }

    #[test]
    fn oversized_remote_allowed_jobs_request_power_on_at_threshold() {
        let decision = decide_capacity_action(
            &JobRequest {
                run_id: "batch-remote-001",
                required_slots: 4,
                priority: JobPriority::Batch,
                allows_remote_capacity: true,
            },
            &CapacitySnapshot {
                local_available_slots: 2,
                remote_online_slots: 0,
                remote_powering_slots: 0,
                idle_seconds: 0,
            },
            &config(),
        );

        assert_eq!(
            decision,
            SchedulingDecision::RequestPowerOn {
                run_id: "batch-remote-001",
                target_group: "worker-group-a",
                slots: 4
            }
        );
    }

    #[test]
    fn maintenance_jobs_hold_for_promotion_window() {
        let decision = decide_capacity_action(
            &JobRequest {
                run_id: "maintenance-001",
                required_slots: 1,
                priority: JobPriority::Maintenance,
                allows_remote_capacity: false,
            },
            &CapacitySnapshot {
                local_available_slots: 2,
                remote_online_slots: 0,
                remote_powering_slots: 0,
                idle_seconds: 0,
            },
            &config(),
        );

        assert_eq!(
            decision,
            SchedulingDecision::Hold {
                run_id: "maintenance-001",
                reason: "maintenance jobs require an explicit promotion window"
            }
        );
    }

    #[test]
    fn idle_remote_workers_request_power_off_after_threshold() {
        let decision = decide_idle_action(
            &CapacitySnapshot {
                local_available_slots: 2,
                remote_online_slots: 4,
                remote_powering_slots: 0,
                idle_seconds: 901,
            },
            &config(),
        );

        assert_eq!(
            decision,
            Some(SchedulingDecision::RequestPowerOff {
                target_group: "worker-group-a",
                idle_seconds: 901
            })
        );
    }

    #[test]
    fn workflow_state_count_matches_public_contract() {
        assert_eq!(workflow_states().len(), 9);
        assert!(workflow_states().contains(&WorkflowState::Deadlettered));
    }

    #[test]
    fn kafka_topics_include_review_and_audit_paths() {
        let topics = kafka_topics();

        assert!(topics.contains(&"agent.review"));
        assert!(topics.contains(&"agent.audit"));
        assert!(topics.contains(&"agent.deadletter"));
    }

    #[test]
    fn power_on_decisions_publish_to_power_commands() {
        let mut broker = InMemoryBroker::default();
        let envelope = publish_scheduling_observation(
            &mut broker,
            &JobRequest {
                run_id: "batch-remote-001",
                required_slots: 4,
                priority: JobPriority::Batch,
                allows_remote_capacity: true,
            },
            &CapacitySnapshot {
                local_available_slots: 2,
                remote_online_slots: 0,
                remote_powering_slots: 0,
                idle_seconds: 0,
            },
            &config(),
        );

        let power_commands = broker.drain_topic(KafkaTopic::PowerCommands);

        assert_eq!(envelope.topic, KafkaTopic::PowerCommands);
        assert_eq!(power_commands.len(), 1);
        assert_eq!(power_commands[0].correlation_id, "batch-remote-001");
        assert!(power_commands[0].payload.contains("request_power_on"));
        assert!(broker.drain_topic(KafkaTopic::AgentCommands).is_empty());
    }

    #[test]
    fn hold_decisions_publish_to_scheduler_capacity() {
        let mut broker = InMemoryBroker::default();
        let envelope = publish_scheduling_observation(
            &mut broker,
            &JobRequest {
                run_id: "small-remote-001",
                required_slots: 2,
                priority: JobPriority::Batch,
                allows_remote_capacity: true,
            },
            &CapacitySnapshot {
                local_available_slots: 0,
                remote_online_slots: 0,
                remote_powering_slots: 0,
                idle_seconds: 0,
            },
            &config(),
        );

        assert_eq!(envelope.topic, KafkaTopic::SchedulerCapacity);
        assert_eq!(
            broker.drain_topic(KafkaTopic::SchedulerCapacity)[0].payload,
            "hold run_id=small-remote-001 reason=below remote wake threshold"
        );
    }

    #[test]
    fn transient_delivery_failure_retries_on_original_topic() {
        let envelope = MessageEnvelope::new(
            KafkaTopic::AgentCommands,
            "interactive-local-001",
            WorkflowState::Scheduled,
            "run_here run_id=interactive-local-001 slots=1".to_string(),
            &config(),
        );

        let retry = next_delivery_attempt(&envelope, DeliveryFailure::Transient("broker timeout"));

        assert_eq!(retry.topic, KafkaTopic::AgentCommands);
        assert_eq!(retry.workflow_state, WorkflowState::Retrying);
        assert_eq!(retry.attempt, 2);
        assert_eq!(retry.correlation_id, envelope.correlation_id);
        assert!(retry.payload.contains("broker timeout"));
    }

    #[test]
    fn exhausted_transient_failure_routes_to_deadletter() {
        let mut envelope = MessageEnvelope::new(
            KafkaTopic::PowerCommands,
            "batch-remote-001",
            WorkflowState::Scheduled,
            "request_power_on run_id=batch-remote-001 target_group=worker-group-a slots=4"
                .to_string(),
            &config(),
        );
        envelope.attempt = envelope.max_attempts;

        let deadletter =
            next_delivery_attempt(&envelope, DeliveryFailure::Transient("broker timeout"));

        assert_eq!(deadletter.topic, KafkaTopic::AgentDeadletter);
        assert_eq!(deadletter.workflow_state, WorkflowState::Deadlettered);
        assert_eq!(deadletter.correlation_id, "batch-remote-001");
        assert!(deadletter.payload.contains("source_topic=power.commands"));
    }

    #[test]
    fn permanent_delivery_failure_routes_to_deadletter_immediately() {
        let envelope = MessageEnvelope::new(
            KafkaTopic::SchedulerCapacity,
            "maintenance-001",
            WorkflowState::Scheduled,
            "hold run_id=maintenance-001 reason=promotion window required".to_string(),
            &config(),
        );

        let deadletter =
            next_delivery_attempt(&envelope, DeliveryFailure::Permanent("invalid envelope"));

        assert_eq!(deadletter.topic, KafkaTopic::AgentDeadletter);
        assert_eq!(deadletter.workflow_state, WorkflowState::Deadlettered);
        assert_eq!(deadletter.attempt, 1);
        assert!(deadletter.payload.contains("invalid envelope"));
    }
}
