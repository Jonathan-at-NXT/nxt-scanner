-- Datenträger entfernen (umkehrbar): falsch angelegt, umbenannt (alter Eintrag), ausgemustert, defekt entsorgt.
-- Ein späterer Scan derselben Platte (last_scan > retired_at) holt sie automatisch zurück.
ALTER TABLE disk_meta ADD COLUMN retired_at TEXT;
ALTER TABLE disk_meta ADD COLUMN retired_reason TEXT;
