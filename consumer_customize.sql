   
use role accountadmin;            
create or replace table "DCR_DEMO_CONSUMER"."MYDATA"."BRAND_CUSTOMER" as select * from "CLEANROOM_SALES"."PRODUCT_SALES"."CUSTOMER";
create or replace table "DCR_DEMO_CONSUMER"."MYDATA"."BRAND_SALES" as select * from "CLEANROOM_SALES"."PRODUCT_SALES"."SALES";
                        
            
select * from "DCR_DEMO_CONSUMER"."LOCAL"."USER_SETTINGS";
  
  
delete from dcr_demo_consumer.local.user_settings;
insert into dcr_demo_consumer.local.user_settings (setting_name, setting_value)
 VALUES ('app_data','dcr_demo_data'),
        ('app_data_two','dcr_demo_data_two'),
        ('consumer_db','dcr_demo_consumer'),
        ('consumer_schema','mydata'),
        ('consumer_table','customers'),
        ('consumer_join_field','email'),
        ('app_instance','dcr_demo_app'),
        ('app_instance_two','dcr_demo_app_two'),
        ('consumer_email_field','email'),
        ('consumer_phone_field','phone'),
        ('consumer_customer_table','customers'),
        ('consumer_conversions_table','conversions'),
        ('consumer_requests_table','dcr_demo_consumer.shared.requests'),
        ('consumer_internal_join_field','email'),
        ('brand_consumer_table','brand_customer'),
        ('brand_consumer_join_field','user_id');
  
select * from "DCR_DEMO_APP"."CLEANROOM"."TEMPLATES";       
        
//call dcr_demo_app.cleanroom.request('audience_overlap',
//object_construct(
//'dimensions',array_construct('c.marital','c.country','c.equipment_like','c.activity_like','c.age_range','c.state','c.lifestyle_like','c.gender','p.genre','p.bundle_user','p.subscription_type','p.product_name'),
//'epsilon', '0.01')::varchar, NULL, NULL);

call dcr_demo_app.cleanroom.request('audience_overlap',
object_construct(
'dimensions',array_construct('c.marital','c.age_range','c.lifestyle_like','c.gender','p.genre'),
'epsilon', '0.01')::varchar, NULL, NULL);
        
select REQUEST_ID, APPROVED, request:PROPOSED_QUERY::varchar from dcr_demo_app.cleanroom.provider_log;
  
select
    identifier('c.marital')
    , identifier('c.age_range')
    , identifier('c.lifestyle_like')
    , identifier('c.gender')
    , identifier('p.genre')
    , count(distinct p.user_id) as actual_overlap 
    , to_number(round(dcr_demo_app.cleanroom.addNoise(count(distinct p.user_id),0.01,dcr_demo_app.cleanroom.ns_306ef26f_3ce4_449f_bbea_8fb453473436()))) as dp_overlap 
from
    dcr_demo_data.cleanroom.provider_publisher p,
    dcr_demo_consumer.mydata.brand_customer at(timestamp => '2022-04-06 12:49:03.331 -0700'::timestamp_tz) c
where
    c.user_id = p.user_id
    and exists (select table_name from dcr_demo_consumer.information_schema.tables where table_schema = upper('mydata') and table_name = upper('customers') and table_type = 'BASE TABLE')
    and ((select any_value(pid) from dcr_demo_data.cleanroom.pid)=(select any_value(pid) from dcr_demo_app.cleanroom.pid))
group by
    identifier('c.marital')
    , identifier('c.age_range')
    , identifier('c.lifestyle_like')
    , identifier('c.gender')
    , identifier('p.genre')
having count(distinct p.user_id)  > 25
order by count(distinct p.user_id) desc;
