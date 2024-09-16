create procedure sp_pfmn_get_missing_index_details (@dbname varchar(100))
as 
set nocount on
select 
    db_name(mid.database_id) as DatabaseName,
    object_name(mid.object_id, mid.database_id) as TableName,
    mid.equality_columns,
    mid.inequality_columns,
    mid.included_columns,
    migs.user_seeks,
    migs.unique_compiles,
    migs.avg_user_impact
from sys.dm_db_missing_index_details as mid
inner join sys.dm_db_missing_index_groups as mig
    on mid.index_handle = mig.index_handle
inner join sys.dm_db_missing_index_group_stats as migs
    on mig.index_group_handle = migs.group_handle
where mid.database_id = db_id(@dbname)
order by migs.avg_user_impact desc

