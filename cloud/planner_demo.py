"""Planer-Demo: berechnet aus den aktuellen D1-Daten einen 'genau 2x'-Plan
(Schützen / Trimmen) + Retention-Sicht. Strukturelles Matching (namensunabhängig).
Nur Lesen — schlägt Aktionen vor, ändert nichts.
"""
import json, subprocess, re
from collections import defaultdict
from datetime import date

TODAY = date(2026, 7, 27)
RETENTION_YEARS = 2   # Demo-Default-Tarif
TB = 1e12
CWD = "/Users/jonathanoehler/Documents/Developer/nxt-scanner/cloud"

def q(sql):
    out = subprocess.run(["wrangler","d1","execute","nxt-scanner","--remote","--json","--command",sql],
                         capture_output=True, text=True, cwd=CWD).stdout
    return json.loads(out[out.find('['):])[0]['results']

disks = {d['uuid']: (d['name'] or d['uuid'][:8]) for d in q("SELECT uuid,name FROM disks")}
tree = q("SELECT disk_uuid,rel_path,size_bytes,file_count FROM folder_tree WHERE depth=1")
items = [(r['disk_uuid'], r['rel_path'], r['size_bytes'] or 0, r['file_count'] or 0)
         for r in tree if (r['size_bytes'] or 0) >= 5e9]
items.sort(key=lambda x: x[2])

# Union-Find: gleiche Daten = Größe ±1% + Dateizahl ±2%
parent = list(range(len(items)))
def find(i):
    while parent[i]!=i: parent[i]=parent[parent[i]]; i=parent[i]
    return i
def union(a,b):
    ra,rb=find(a),find(b)
    if ra!=rb: parent[rb]=ra
for i in range(len(items)):
    si=items[i][2]; k=i-1
    while k>=0 and items[k][2]>=si*0.99:
        if abs(si-items[k][2])<=si*0.01 and (max(items[i][3],items[k][3])==0 or
           abs(items[i][3]-items[k][3])<=max(items[i][3],items[k][3])*0.02):
            union(i,k)
        k-=1
clusters=defaultdict(list)
for i in range(len(items)): clusters[find(i)].append(i)

def proj_date(name):
    m=re.search(r'(\d{4})[-_](\d{2})[-_](\d{2})', name)
    if m:
        try: return date(int(m[1]),int(m[2]),int(m[3]))
        except: return None
    return None

protect=[]; trim=[]; expired=[]
for root, idxs in clusters.items():
    ds = {items[i][0] for i in idxs}
    size = sorted(items[i][2] for i in idxs)[len(idxs)//2]  # median
    name = items[idxs[0]][1]
    src = disks.get(items[idxs[0]][0], '?')
    d = proj_date(name)
    keep_until = date(d.year+RETENTION_YEARS, d.month, min(d.day,28)) if d else None
    if len(ds)==1:
        protect.append((size, name, src, items[idxs[0]][0]))
    elif len(ds)>2:
        trim.append(((len(ds)-2)*size, len(ds), name))
    if keep_until and keep_until < TODAY:
        expired.append((size, name, keep_until))

protect.sort(reverse=True); trim.sort(reverse=True); expired.sort(reverse=True)

print("="*64)
print("NXT SPEICHER-PLANER (Demo, strukturell, aktuelle D1-Daten)")
print("="*64)
prot_tb=sum(s for s,*_ in protect)
print(f"\n■ SCHÜTZEN — {len(protect)} Projekte mit nur 1 Kopie ({prot_tb/TB:.1f} TB)")
print("  (Priorität 1: 2. Kopie anlegen, VOR jeder Löschung)")
# Sessions: gruppiert nach Quell-Platte
by_src=defaultdict(lambda:[0,0])
for s,name,src,uuid in protect: by_src[src][0]+=s; by_src[src][1]+=1
print("  → Sessions (Platte anstecken, alle Einzelkopien duplizieren):")
for src,(tb,n) in sorted(by_src.items(), key=lambda x:-x[1][0])[:10]:
    print(f"     stecke {src:14s} an → {n:2d} Projekte, {tb/TB:5.1f} TB sichern")
print("  Größte ungesicherte:")
for s,name,src,_ in protect[:6]:
    print(f"     {s/1e9:6.0f} GB  {name[:38]:38s} @ {src}")

trim_tb=sum(t for t,*_ in trim)
print(f"\n■ TRIMMEN — {len(trim)} Projekte mit >2 Kopien → {trim_tb/TB:.1f} TB rückgewinnbar")
print("  (Priorität 2: nach 2 verifizierten Kopien überzählige löschen)")
for t,n,name in trim[:6]:
    print(f"     {t/1e9:6.0f} GB frei  {name[:36]:36s} ({n} Kopien)")

exp_tb=sum(s for s,*_ in expired)
print(f"\n■ RETENTION (Demo-Tarif {RETENTION_YEARS} J) — {len(expired)} Projekte abgelaufen → {exp_tb/TB:.1f} TB rückgewinnbar")
for s,name,ku in expired[:6]:
    print(f"     {s/1e9:6.0f} GB  {name[:36]:36s} keep_until {ku}")

print("\n"+"-"*64)
print("BILANZ")
print(f"  zu sichern (Bedarf):      +{prot_tb/TB:6.1f} TB Kopiervolumen")
print(f"  rückgewinnbar (Trim):     -{trim_tb/TB:6.1f} TB")
print(f"  rückgewinnbar (Retention):-{exp_tb/TB:6.1f} TB")
net = prot_tb - trim_tb - exp_tb
print(f"  Netto-Speicheränderung:   {net/TB:+6.1f} TB", "(passt ins Budget)" if net<0 else "(Mehrbedarf)")

json.dump({"protect_tb":round(prot_tb/TB,1),"trim_tb":round(trim_tb/TB,1),
           "expired_tb":round(exp_tb/TB,1),
           "sessions":[{"disk":s,"n":n,"tb":round(t/TB,1)} for s,(t,n) in
                       sorted(by_src.items(),key=lambda x:-x[1][0])]},
          open("/private/tmp/claude-501/-Users-jonathanoehler/c81965ec-20b9-4919-88de-4fc76c3801bb/scratchpad/plan.json","w"))
