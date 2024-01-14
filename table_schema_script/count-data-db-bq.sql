select count(*) as pm from pmr_pm_plan  where updated_at AT time zone 'utc' >='2019-01-01 00:00:00';
select count(*) as pm_item from pmr_pm_item where updated_at AT time zone 'utc' >='2019-01-01 00:00:00';
select count(*) as inventoy from pmr_inventory where  is_dummy=false and updated_at AT time zone 'utc' >='2019-01-01 00:00:00';
select count(*) as project from pmr_project where  is_dummy=false and updated_at AT time zone 'utc' >='2019-01-01 00:00:00';
