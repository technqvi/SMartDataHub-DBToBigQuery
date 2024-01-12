select count(*) from app_preventivemaintenance where updated_at AT time zone 'utc' >='2019-01-01 00:00:00';
select count(*) from app_pm_inventory where updated_at AT time zone 'utc' >='2019-01-01 00:00:00';
select count(*) from app_inventory where  is_dummy=false and updated_at AT time zone 'utc' >='2019-01-01 00:00:00';
select count(*) from app_project where  is_dummy=false and updated_at AT time zone 'utc' >='2019-01-01 00:00:00';