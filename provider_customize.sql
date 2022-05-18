//create table in publisher shared database
create or replace table "DCR_DEMO_PROVIDER_DB"."SHARED_SCHEMA"."PUBLISHER_CUSTOMER" as select b.*,  a.genre, a.affinity_score
from "CLEANROOM_AUDIENCE_SAMPLE"."SOURCE"."AFFINITY" a inner join "CLEANROOM_AUDIENCE_SAMPLE"."SOURCE"."SUBSCRIPTION" b
on a.rec_id = b.rec_id;

//create secure view 
create or replace secure view "DCR_DEMO_PROVIDER_DB"."CLEANROOM"."PROVIDER_PUBLISHER" as select * from "DCR_DEMO_PROVIDER_DB"."SHARED_SCHEMA"."PUBLISHER_CUSTOMER";

//apply row access policy
ALTER TABLE  "DCR_DEMO_PROVIDER_DB"."CLEANROOM"."PROVIDER_PUBLISHER" add row access policy dcr_demo_provider_db.shared_schema.data_firewall on (email);

//grant select on secure view to share
grant select on "DCR_DEMO_PROVIDER_DB"."CLEANROOM"."PROVIDER_PUBLISHER" to share dcr_demo_data;



//insert new template

insert into dcr_demo_provider_db.templates.dcr_templates (template_name, template, dp_sensitivity, dimensions) 
values ('audience_overlap',
$$
select
    identifier({{ dimensions[0] }})
    {% for dim in dimensions[1:] %}
    , identifier({{ dim  }})
    {% endfor %}
    , count(distinct p.user_id) as actual_overlap 
    , to_number(round({{ app_instance | sqlsafe }}.cleanroom.addNoise(count(distinct p.user_id),{{ epsilon | sqlsafe }},{{ app_instance | sqlsafe }}.cleanroom.ns_{{ request_id | sqlsafe }}()))) as dp_overlap 
from
    {{ app_data | sqlsafe }}.cleanroom.provider_publisher p,
    {{ consumer_db | sqlsafe }}.{{ consumer_schema | sqlsafe }}.{{ brand_consumer_table | sqlsafe }} at(timestamp => '{{ at_timestamp | sqlsafe }}'::timestamp_tz) c
where
    c.{{ brand_consumer_join_field | sqlsafe }} = p.user_id
    and exists (select table_name from {{ consumer_db | sqlsafe }}.information_schema.tables where table_schema = upper('{{ consumer_schema | sqlsafe }}') and table_name = upper('{{ consumer_table| sqlsafe }}') and table_type = 'BASE TABLE')
    and ((select any_value(pid) from {{ app_data | sqlsafe }}.cleanroom.pid)=(select any_value(pid) from {{ app_instance | sqlsafe }}.cleanroom.pid))
   {% if  where_clause  %} 
    and ( {{ where_clause | sqlsafe }} )
    {% endif %}    
group by
    identifier({{ dimensions[0]  }})
    {% for dim in dimensions[1:] %}
    , identifier({{ dim }})
    {% endfor %}
having count(distinct p.user_id)  > 25
order by count(distinct p.user_id) desc;
$$,1,'c.country|c.marital|c.equipment_like|c.activity_like|c.age_range|c.state|c.lifestyle_like|c.gender|p.genre|p.bundle_user|p.subscription_type|p.product_name');

select * from dcr_demo_provider_db.templates.dcr_templates ;
