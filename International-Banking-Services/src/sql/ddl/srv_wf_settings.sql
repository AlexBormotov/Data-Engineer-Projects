DROP TABLE IF EXISTS AVBORMOTOVYANDEXRU__STAGING.srv_wf_settings CASCADE;

CREATE TABLE AVBORMOTOVYANDEXRU__STAGING.srv_wf_settings (
	id uuid PRIMARY KEY DISABLED,
	workflow_key varchar NOT NULL,
	workflow_settings varchar NOT NULL,
	CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);

CREATE PROJECTION AVBORMOTOVYANDEXRU__STAGING.srv_wf_settings
(
    id,
    workflow_key,
    workflow_settings
)
AS
 SELECT srv_wf_settings.id,
        srv_wf_settings.workflow_key,
        srv_wf_settings.workflow_settings
 FROM AVBORMOTOVYANDEXRU__STAGING.srv_wf_settings
 ORDER BY srv_wf_settings.id
SEGMENTED BY hash(srv_wf_settings.id) ALL NODES KSAFE 1;