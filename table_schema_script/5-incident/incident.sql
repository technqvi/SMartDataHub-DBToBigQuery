create view xyz_incident
            (incident_id, inventory_id, incident_type, service_type, severity, status, open_datetime, close_datetime,
             incident_owner, updated_at)
as
SELECT abc.id                                                                                      AS incident_id,
       abc.inventory_id,
       (SELECT x.incident_type_name
        FROM app_incident_type x
        WHERE x.id = abc.incident_type_id)                                                         AS incident_type,
       (SELECT x.service_type_name
        FROM app_service_type x
        WHERE x.id = abc.service_type_id)                                                          AS service_type,
       (SELECT x.severity_name
        FROM app_incident_severity x
        WHERE x.id = abc.incident_severity_id)                                                     AS severity,
       (SELECT x.incident_status_name
        FROM app_incident_status x
        WHERE x.id = abc.incident_status_id)                                                       AS status,
       to_char((abc.incident_datetime AT TIME ZONE 'UTC'::text), 'YYYY-MM-DD HH24:MI'::text)       AS open_datetime,
       to_char((abc.incident_close_datetime AT TIME ZONE 'UTC'::text), 'YYYY-MM-DD HH24:MI'::text) AS close_datetime,
       (SELECT x.employee_name
        FROM app_employee x
        WHERE x.id = abc.incident_owner_id)                                                        AS incident_owner,
       abc.updated_at
FROM app_incident abc
WHERE abc.incident_status_id <> 3;

alter table xyz_incident
    owner to postgres;

