create procedure sp_pfmn_get_costly_missing_indexes
as set nocount on
select 
    migs.group_handle,
    migs.avg_total_user_cost,
    migs.avg_user_impact,
    migs.user_seeks,
    migs.user_scans,
    db_name(mid.database_id) as database_name,
    object_name(mid.object_id, mid.database_id) as table_name,
    mid.equality_columns,
    mid.inequality_columns,
    mid.included_columns
from sys.dm_db_missing_index_group_stats migs
inner join sys.dm_db_missing_index_groups mig
    on migs.group_handle = mig.index_group_handle
inner join sys.dm_db_missing_index_details mid
    on mig.index_handle = mid.index_handle
order by migs.avg_user_impact desc

