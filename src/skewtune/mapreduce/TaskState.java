package skewtune.mapreduce;

public enum TaskState {
    RUNNING,
    PREPARE_TAKEOVER,
    TAKEOVER, // successfully takeovered
    CANCEL_TAKEOVER,
    COMPLETE,
    WAIT_TAKEOVER, // waiting for takeover
    TAKEOVER_COMPLETE // takenover task has completed. waiting for subtask completes
}