-- Sicherungsregel je Ordnertyp eines Projekts (JSON {"PHOTOS": "loeschbar", ...}); überschreibt die Projektregel für diesen Typ.
ALTER TABLE project_rule ADD COLUMN type_sicherung TEXT;
