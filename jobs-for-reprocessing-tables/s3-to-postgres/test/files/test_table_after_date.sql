select 
event_category
,event_action
,event_label
,total_events
,unique_events
,event_value
,avg_event_value
,cast(year(date('2022-05-17')) AS varchar) "year"
,lpad(cast(month(date('2022-05-17')) AS varchar), 2, '0') "month" 
,lpad(cast(day(date('2022-05-17')) AS varchar), 2, '0') "day"
from test_table_source
where date(concat(year,'-',month,'-',day))="date_add"('DAY', -date_diff('day', date('2022-05-17'), current_date), current_date)
and event_action like '%REQUEST%'