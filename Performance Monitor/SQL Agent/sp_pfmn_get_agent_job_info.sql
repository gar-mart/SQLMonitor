create procedure sp_pfmn_get_agent_job_info
    @execution_start_date date = null,
    @job_owner nvarchar(128) = null,
    @run_status int = null,
    @step_name nvarchar(128) = null
as set nocount on
begin
    -- Jobs sorted by execution start time
    select j.name as job_name, j.description, a.start_execution_date
    from msdb.dbo.sysjobs j
    inner join msdb.dbo.sysjobactivity a on j.job_id = a.job_id
    where (@execution_start_date is null or a.start_execution_date > @execution_start_date)
    and j.enabled = 1
    order by a.start_execution_date;

    -- Jobs with their owners
    select a.name as job_name, b.name as job_owner
    from msdb.dbo.sysjobs_view a
    left join master.dbo.syslogins b on a.owner_sid = b.sid
    where (@job_owner is null or b.name = @job_owner);

    -- Failed agent jobs
    select j.name as job_name, js.step_name, jh.sql_severity, jh.message, jh.run_date, jh.run_time
    from msdb.dbo.sysjobs as j
    inner join msdb.dbo.sysjobsteps as js on js.job_id = j.job_id
    inner join msdb.dbo.sysjobhistory as jh on jh.job_id = j.job_id 
    where (@run_status is null or jh.run_status = @run_status);

    -- Jobs with specific step name
    select j.name as job_name, js.step_name, jh.sql_severity, jh.message, jh.run_date, jh.run_time
    from msdb.dbo.sysjobs as j
    inner join msdb.dbo.sysjobsteps as js on js.job_id = j.job_id
    inner join msdb.dbo.sysjobhistory as jh on jh.job_id = j.job_id 
    where (@step_name is null or js.step_name = @step_name);
end;
