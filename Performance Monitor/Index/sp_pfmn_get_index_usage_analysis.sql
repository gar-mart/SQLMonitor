create procedure sp_pfmn_get_index_usage_analysis(@database_name varchar(100) null)
as 
set nocount on
select 
    object_name(ius.object_id) as tablename,
    i.name as indexname,
    ius.user_seeks,
    ius.user_scans,
    ius.user_lookups,
    ius.user_updates,
    db_name(ius.database_id) as database_name,
    i.index_id,
    ius.user_seeks,
    ius.user_scans,
    ius.user_lookups,
    ius.user_updates,
    ius.last_user_seek,
    ius.last_user_scan,
    ius.last_user_lookup,
    ius.last_user_update
from sys.dm_db_index_usage_stats as ius
inner join sys.indexes as i
    on ius.object_id = i.object_id and ius.index_id = i.index_id
where ius.database_id != db_id(@database_name) or @database_name is null
order by ius.user_seeks desc;
