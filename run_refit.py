from engine.edge_floors import compute_all_sports
out = compute_all_sports()
shadowed = []
for sport, cells in out.items():
    for k, v in cells.items():
        if v.get("shadow"):
            shadowed.append((sport, k, v.get("n_all"), v.get("roi_all"), v.get("n60"), v.get("roi60")))
print("SHADOWED CELLS (%d):" % len(shadowed))
for s in sorted(shadowed):
    print("  %s %s n=%s roi=%s n60=%s roi60=%s" % s)
