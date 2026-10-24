-- Sollstand je Ordnertyp: welche Kopie gilt als "vollständig" (JSON {"FOOTAGE": "<disk_uuid>", ...}).
-- Nötig, wenn bewusst Material entfernt wurde (z. B. SPEC X: Recorder-Daten gelöscht) und die größte Kopie nicht der Sollstand ist.
ALTER TABLE project_rule ADD COLUMN ref_copies TEXT;
