-- Chatverlauf je Person: thread = E-Mail (Dashboard-Login bzw. Slack-Profil) oder slack:<User-ID>
ALTER TABLE chat_message ADD COLUMN thread TEXT;
UPDATE chat_message SET thread = lower(author) WHERE role = 'user' AND thread IS NULL;
UPDATE chat_message SET thread = (SELECT u.thread FROM chat_message u WHERE u.role = 'user' AND u.id < chat_message.id ORDER BY u.id DESC LIMIT 1) WHERE role = 'agent' AND thread IS NULL;
CREATE INDEX IF NOT EXISTS idx_chat_thread ON chat_message(thread, id);
