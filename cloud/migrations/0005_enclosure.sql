-- Gehäuse: Platten, die gemeinsam an EINEM Anschluss hängen (z. B. NH 01–03), zählen als ein Anschluss
ALTER TABLE disk_meta ADD COLUMN enclosure TEXT;
