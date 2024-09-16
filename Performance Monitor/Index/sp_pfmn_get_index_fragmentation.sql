create procedure sp_pfmn_get_index_fragmentation
    @database_name     varchar(100),  -- Name of database
    @min_frag          int,           -- Minimum fragmentation value
    @table_size        bigint,        -- Table size in pages
    @print_command     bit = 0,       -- Flag to print command
    @execute_command   bit = 1        -- Flag to execute command
as
set nocount on    
begin    
   declare @command nvarchar(4000)   

  drop table if exists ##fragmentation_details  

        create table ##fragmentation_details  
        (
            database_name                 varchar(130),
            object_name                   varchar(130),    
            index_name                    varchar(130),    
            schema_name                   varchar(130),    
            avg_fragmentation_percent     float,    
            index_type_desc               varchar(50),  
            allocation_unit_type          varchar(50),
            has_large_data_type           int,
            table_size                    bigint
        )  
        drop table if exists #database_check    

    create table #database_check  
    (  
        database_name  varchar(100),  
        database_id    int
    )  

    insert into #database_check  
    select name, dbid  
    from master.dbo.sysdatabases   
    where name = case when len(isnull(@database_name, '')) = 0 then name else @database_name end 
      and name not in ('master', 'msdb', 'model') 
      and name not like '%temp%' 
      and name not like '%tmp%' 
      and name not like '%train%' 
      and databasepropertyex(name, 'Status') = 'ONLINE'

    -- Loop through each database in #database_check
    declare @current_database_name varchar(100);
    declare @current_database_id int;

    declare @index int = 1;
    declare @total_databases int = (select count(*) from #database_check);

    while @index <= @total_databases
    begin
	;with cte as(
		select 
			database_name,
			database_id,
			row_number() over (order by database_name) rn
        from #database_check

		)
        select @current_database_name = database_name, @current_database_id = database_id
		from cte
        where rn = @index


        set @command = 'select '''+ @current_database_name + ''' as database_name
                            ,O.name as object_name
                            ,I.name as index_name
                            ,S.name as schema_name
                            ,avg_fragmentation_in_percent
                            ,V.index_type_desc as index_type_desc
                            ,alloc_unit_type_desc
                            ,isnull(SQ.object_id, 1) as has_large_data_type
                            ,sum(total_pages) as table_size
                        from sys.dm_db_index_physical_stats ('+ cast(db_id(@current_database_name) as varchar(3)) +', null, null, null, null) V
                        inner join ['+ @current_database_name +'].sys.objects O on V.object_id = O.object_id
                        inner join ['+ @current_database_name +'].sys.schemas S on S.schema_id = O.schema_id
                        inner join ['+ @current_database_name +'].sys.indexes I on I.object_id = O.object_id
                          and V.index_id = I.index_id
                        inner join ['+ @current_database_name +'].sys.partitions P on P.object_id = O.object_id
                        inner join ['+ @current_database_name +'].sys.allocation_units A on P.partition_id = A.container_id
                        left join (select distinct A.object_id
                                   from ['+ @current_database_name +'].sys.columns A
                                   join ['+ @current_database_name +'].sys.types B on A.user_type_id = B.user_type_id
                                   where (B.name in (''type'', ''text'', ''ntext'', ''image'', ''xml'') 
                                          or (B.name in (''varchar'', ''nvarchar'', ''varbinary'') and A.max_length = -1))
                                   ) SQ on SQ.object_id = O.object_id
                        where avg_fragmentation_in_percent >= '+ cast(@min_frag as varchar(8)) + '
                          and I.index_id > 0
                          and I.is_disabled = 0
                          and I.is_hypothetical = 0
                        group by O.name, I.name, S.name, avg_fragmentation_in_percent, V.index_type_desc, alloc_unit_type_desc, isnull(SQ.object_id, 1)
                        having sum(total_pages) >= ' + cast(@table_size as varchar(50)) + ''

        if @print_command = 1
            print  @command  
        
        insert into ##fragmentation_details     
            (database_name, object_name, index_name, schema_name, avg_fragmentation_percent, index_type_desc, allocation_unit_type, has_large_data_type, table_size)
        exec(@command)    

        set @index = @index + 1;

		select * from ##fragmentation_details

    end  
end
