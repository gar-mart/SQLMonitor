create procedure sp_pfmn_get_current_locks
as set nocount on
select request_session_id as session_id, 
       db_name(resource_database_id) as database_name,
       resource_associated_entity_id, 
       case when resource_type = 'object' 
            then object_name(resource_associated_entity_id) 
            when resource_associated_entity_id = 0 then 'n/a' 
            else object_name(p.object_id) 
       end as entity_name, 
       index_id, 
       resource_type as resource, 
       resource_description as description, 
       request_mode as mode, 
       request_status as status 
from sys.dm_tran_locks t 
        left join sys.partitions p 
                   on p.partition_id = t.resource_associated_entity_id 
where   resource_database_id = db_id() 
        and resource_type != 'DATABASE'



