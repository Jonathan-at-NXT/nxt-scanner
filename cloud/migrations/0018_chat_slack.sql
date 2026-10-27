-- Chat mit dem NXT Data Agent auch über Slack: Herkunft der Nachricht, damit die Antwort in denselben Slack-Chat geht
ALTER TABLE chat_message ADD COLUMN slack_event TEXT;
ALTER TABLE chat_message ADD COLUMN slack_channel TEXT;
ALTER TABLE chat_message ADD COLUMN slack_ts TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_slack_event ON chat_message(slack_event) WHERE slack_event IS NOT NULL;
