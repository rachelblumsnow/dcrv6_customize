use role accountadmin;
use warehouse app_wh;           
create or replace table "DCR_DEMOCUR_CONSUMER"."MYDATA"."BRAND_CUSTOMER" as select * from "CLEANROOM_SALES"."PRODUCT_SALES"."CUSTOMER";
create or replace table "DCR_DEMOCUR_CONSUMER"."MYDATA"."BRAND_SALES" as select * from "CLEANROOM_SALES"."PRODUCT_SALES"."SALES";
create or replace secure view dcr_DEMOCUR_consumer.shared.BRAND_CUSTOMER as select * from dcr_DEMOCUR_consumer.mydata.BRAND_CUSTOMER;
create or replace secure view dcr_DEMOCUR_consumer.shared.BRAND_SALES as select * from dcr_DEMOCUR_consumer.mydata.BRAND_SALES;
grant select on table dcr_DEMOCUR_consumer.shared.BRAND_CUSTOMER to role dcr_DEMOCUR_to_app_role;
grant select on table dcr_DEMOCUR_consumer.shared.BRAND_SALES to role dcr_DEMOCUR_to_app_role;

//select * from dcr_DEMOCUR_app.cleanroom.templates;
                        
            
//select * from "DCR_DEMOCUR_CONSUMER"."LOCAL"."USER_SETTINGS";
  
  
//delete from dcr_DEMOCUR_consumer.local.user_settings;
insert into dcr_DEMOCUR_consumer.local.user_settings (setting_name, setting_value)
 VALUES 
        ('brand_consumer_table','brand_customer'),
        ('brand_consumer_join_field','user_id');
        
  
select * from "DCR_DEMOCUR_APP"."CLEANROOM"."TEMPLATES";       
        
//call dcr_DEMOCUR_app.allowed_sprocs.request('audience_overlap',
//object_construct(
//'dimensions',array_construct('c.marital','c.country','c.equipment_like','c.activity_like','c.age_range','c.state','c.lifestyle_like','c.gender','p.genre','p.bundle_user','p.subscription_type','p.product_name'),
//'epsilon', '0.01'));

call dcr_DEMOCUR_app.allowed_sprocs.run('audience_overlap',
object_construct(
'dimensions',array_construct('c.marital','c.age_range','c.lifestyle_like','c.gender','p.genre'),
'epsilon', '0.01'));