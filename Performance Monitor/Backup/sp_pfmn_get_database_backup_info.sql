create procedure sp_pfmn_get_database_backup_info
as set nocount on
select
      d.database_id,
      d.name,
      d.state_desc,
      d.recovery_model_desc,
      data_size = cast(sum(case when mf.[type] = 0 then mf.size end) * 8. / 1024 as decimal(18,2)), -- Data file size in MB
      log_size = cast(sum(case when mf.[type] = 1 then mf.size end) * 8. / 1024 as decimal(18,2)), -- Log file size in MB
      bu.full_last_date,
      bu.full_size,
      bu.log_last_date,
      bu.log_size
from sys.databases d
join sys.master_files mf on d.database_id = mf.database_id
left join (
    select
          database_name,
          full_last_date = max(case when [type] = 'D' then backup_finish_date end),
          full_size = max(case when [type] = 'D' then backup_size end),
          log_last_date = max(case when [type] = 'L' then backup_finish_date end),
          log_size = max(case when [type] = 'L' then backup_size end)
    from msdb.dbo.backupset
    where [type] in ('D', 'L')
    group by database_name
) bu on d.name = bu.database_name
group by
    d.database_id, d.name, d.state_desc, d.recovery_model_desc, bu.full_last_date, bu.full_size, bu.log_last_date, bu.log_size
order by data_size desc