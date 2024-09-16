create procedure sp_pfmn_get_active_transactions
as set nocount on
select
    s_tst.session_id,
    s_es.login_name,
    db_name(s_tdt.database_id), 
    s_tdt.database_transaction_begin_time as begin_time,
    s_tdt.database_transaction_log_bytes_used as log_bytes,
    s_tdt.database_transaction_log_bytes_reserved as log_bytes_reserved,
    s_est.text as sql_text,
    s_eqp.query_plan as last_plan
from
    sys.dm_tran_database_transactions s_tdt
join
    sys.dm_tran_session_transactions s_tst
on
    s_tst.transaction_id = s_tdt.transaction_id
join
    sys.dm_exec_sessions s_es
on
    s_es.session_id = s_tst.session_id
join
    sys.dm_exec_connections s_ec
on
    s_ec.session_id = s_tst.session_id
left outer join
    sys.dm_exec_requests s_er
on
    s_er.session_id = s_tst.session_id
cross apply
    sys.dm_exec_sql_text(s_ec.most_recent_sql_handle) as s_est
outer apply
    sys.dm_exec_query_plan(s_er.plan_handle) as s_eqp
order by
    begin_time






