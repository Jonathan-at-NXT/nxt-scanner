-- Dropbox-Ausstieg als Ordnerbaum: Elternordner + Tiefe (0 = Bereich, z. B. "NXT STUDIOS/CLIENTS")
ALTER TABLE dropbox_folder ADD COLUMN parent TEXT;
ALTER TABLE dropbox_folder ADD COLUMN depth INTEGER;
