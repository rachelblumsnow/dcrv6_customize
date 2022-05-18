use role accountadmin;
use warehouse app_wh;   

//create table in publisher shared database
create or replace table "DCR_DEMOCUR_PROVIDER_DB"."SHARED_SCHEMA"."PUBLISHER_CUSTOMER" as select b.*,  a.genre, a.affinity_score
from "CLEANROOM_AUDIENCE_SAMPLE"."SOURCE"."AFFINITY" a inner join "CLEANROOM_AUDIENCE_SAMPLE"."SOURCE"."SUBSCRIPTION" b
on a.rec_id = b.rec_id;

//create secure view 
create or replace secure view "DCR_DEMOCUR_PROVIDER_DB"."SHARED_SCHEMA"."PROVIDER_PUBLISHER" as select * from "DCR_DEMOCUR_PROVIDER_DB"."SHARED_SCHEMA"."PUBLISHER_CUSTOMER";

//apply row access policy
ALTER TABLE  "DCR_DEMOCUR_PROVIDER_DB"."SHARED_SCHEMA"."PROVIDER_PUBLISHER" add row access policy dcr_DEMOCUR_provider_db.shared_schema.data_firewall on (email);

//grant select on secure view to share
grant select on "DCR_DEMOCUR_PROVIDER_DB"."CLEANROOM"."PROVIDER_PUBLISHER" to share dcr_DEMOCUR_data;
grant select on "DCR_DEMOCUR_PROVIDER_DB"."SHARED_SCHEMA"."PROVIDER_PUBLISHER" to share dcr_DEMOCUR_app; 

select * from dcr_DEMOCUR_provider_db.templates.dcr_templates;

//insert new template

insert into dcr_DEMOCUR_provider_db.templates.dcr_templates (template_name, template, dp_sensitivity, dimensions, template_type) 
values ('audience_overlap',
$$
select
{% if dimensions %}
    identifier({{ dimensions[0] }})
    {% for dim in dimensions[1:] %}
    , identifier({{ dim  }})
    {% endfor %}
    , {% endif %}  
    round(cleanroom.addNoise(count(distinct p.email),{{ epsilon | sqlsafe }},{{ app_instance | sqlsafe }}.cleanroom.ns_{{ request_id | sqlsafe }}())) as dp_overlap 
from
    shared_schema.provider_publisher p,
    {{ consumer_db | sqlsafe }}.{{ consumer_shared_data_schema | sqlsafe }}.{{ brand_consumer_table | sqlsafe }} at(timestamp => '{{ at_timestamp | sqlsafe }}'::timestamp_tz) c
where
    c.{{ brand_consumer_join_field | sqlsafe }} = p.user_id
    {% if  where_clause  %} 
    and ( {{ where_clause | sqlsafe }} )
    {% endif %}    
{% if dimensions %}
    group by identifier({{ dimensions[0] }})
    {% for dim in dimensions[1:] %}
    , identifier({{ dim  }})
    {% endfor %}
    {% endif %} 
having dp_overlap  > 25
order by dp_overlap desc;
$$,1,'c.country|c.marital|c.equipment_like|c.activity_like|c.age_range|c.state|c.lifestyle_like|c.gender|p.genre|p.bundle_user|p.subscription_type|p.product_name','SQL_immediate');

//delete from dcr_DEMOCUR_provider_db.templates.dcr_templates where template_name = 'audience_overlap';
