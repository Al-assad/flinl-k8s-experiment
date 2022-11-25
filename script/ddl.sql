create table if not exists flink_job_overview_hist
(
    cluster_id        text,
    namespace         text,
    job_id            text,
    job_name          text,
    state             text,
    start_ts          bigint,
    end_ts            bigint,
    task_total        int,
    task_created      int,
    task_scheduled    int,
    task_deploying    int,
    task_running      int,
    task_finished     int,
    task_canceling    int,
    task_canceled     int,
    task_failed       int,
    task_reconciling  int,
    task_initializing int,
    ts                bigint,
    primary key (cluster_id, namespace, job_id, ts)
);
