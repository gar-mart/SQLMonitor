create procedure sp_pfmn_detect_long_running_jobs(
    @deviation_times int = 3,
    @default_max_duration_minutes int = 15,
    @from_address nvarchar(128),
    @recipients nvarchar(128)
)
as set nocount on
if object_id('dbo.JobMaxDurationSetting') is null
begin
    create table dbo.JobMaxDurationSetting(
        JobName sysname not null,
        MaxDurationMinutes int not null
    )
end

declare
    @start_exec_count int = 5,
    @subject nvarchar(255) = 'Long Running Job Detected On ' + host_name(),
    @body nvarchar(max)

declare @RunningJobs table (
    job_id uniqueidentifier not null,
    last_run_date int not null,
    last_run_time int not null,
    next_run_date int not null,
    next_run_time int not null,
    next_run_schedule_id int not null,
    requested_to_run int not null,
    request_source int not null,
    request_source_id sysname null,
    running int not null,
    current_step int not null,
    current_retry_attempt int not null,
    job_state int not null
)

declare @DetectedJobs table(
    JobId uniqueidentifier,
    JobName sysname,
    ExecutionDate datetime,
    AvgDuration int,
    MaxDuration int,
    CurrentDuration int
)

insert into @RunningJobs
exec master.dbo.xp_sqlagent_enum_jobs 1, ''

;with JobsHistory as (
    select
        job_id,
        dbo.agent_datetime(run_date, run_time) as DateExecuted,
        run_duration / 10000 * 3600 + run_duration % 10000 / 100 * 60 + run_duration % 100 as Duration
    from dbo.sysjobhistory
    where step_id = 0
    and run_status = 1
),
JobHistoryStats as (
    select
        job_id,
        avg(Duration * 1.0) as AvgDuration,
        avg(Duration * 1.0) * @deviation_times as MaxDuration
    from JobsHistory
    group by job_id
    having count(*) >= @start_exec_count
)
insert into @DetectedJobs(
    JobId,
    JobName,
    ExecutionDate,
    AvgDuration,
    MaxDuration,
    CurrentDuration
)
select
    a.job_id as JobId,
    c.name as JobName,
    max(e.start_execution_date) as ExecutionDate,
    b.AvgDuration,
    isnull(max(i.MaxDurationMinutes) * 60, b.MaxDuration) as MaxDuration,
    max(datediff(second, e.start_execution_date, getdate())) as CurrentDuration
from JobsHistory a
inner join JobHistoryStats b on a.job_id = b.job_id
inner join dbo.sysjobs c on a.job_id = c.job_id
inner join @RunningJobs d on d.job_id = a.job_id
inner join dbo.sysjobactivity e on e.job_id = a.job_id
and e.stop_execution_date is null
and e.start_execution_date is not null
left join dbo.JobMaxDurationSetting i on i.JobName = c.name
where datediff(second, e.start_execution_date, getdate()) > isnull(i.MaxDurationMinutes * 60, (select max(d) from (values(b.MaxDuration), (@default_max_duration_minutes * 60)) v(d)))
and d.job_state = 1
group by a.job_id, c.name, b.AvgDuration, b.MaxDuration

if @@rowcount = 0
    return

declare
    @JobId uniqueidentifier,
    @JobName sysname,
    @ExecutionDate datetime,
    @AvgDuration int,
    @MaxDuration int,
    @CurrentDuration int

declare job_cursor cursor local fast_forward for
select JobId, JobName, ExecutionDate, AvgDuration, MaxDuration, CurrentDuration
from @DetectedJobs
order by CurrentDuration desc

open job_cursor

fetch next from job_cursor into @JobId, @JobName, @ExecutionDate, @AvgDuration, @MaxDuration, @CurrentDuration
set @body = 'Long Running Jobs Detected On Server ' + cast(host_name() as varchar(128)) + char(13) + char(10) + char(13) + char(10)

while @@fetch_status = 0
begin
    set @body += 'Job Name: ' + cast(@JobName as varchar(128)) + '  (ID: ' + cast(@JobId as char(36)) + ')' + char(13) + char(10)
    set @body += 'StartDate: ' + cast(@ExecutionDate as varchar(25)) + char(13) + char(10)
    set @body += 'Current Duration: ' + cast(@CurrentDuration / 3600 as varchar(10)) + ':' + right('00' + cast(@CurrentDuration % 3600 / 60 as varchar(2)), 2) + ':' + right('00' + cast(@CurrentDuration % 60 as varchar(2)), 2) + char(13) + char(10)
    set @body += 'Average Duration: ' + cast(@AvgDuration / 3600 as varchar(10)) + ':' + right('00' + cast(@AvgDuration % 3600 / 60 as varchar(2)), 2) + ':' + right('00' + cast(@AvgDuration % 60 as varchar(2)), 2) + char(13) + char(10)
    set @body += 'Max Duration: ' + cast(@MaxDuration / 3600 as varchar(10)) + ':' + right('00' + cast(@MaxDuration % 3600 / 60 as varchar(2)), 2) + ':' + right('00' + cast(@MaxDuration % 60 as varchar(2)), 2) + char(13) + char(10)
    set @body += char(13) + char(10) + char(13) + char(10)

    fetch next from job_cursor into @JobId, @JobName, @ExecutionDate, @AvgDuration, @MaxDuration, @CurrentDuration
end

close job_cursor
deallocate job_cursor

exec dbo.sp_send_dbmail
    @from_address = @from_address,
    @recipients = @recipients,
    @subject = @subject,
    @body = @body
