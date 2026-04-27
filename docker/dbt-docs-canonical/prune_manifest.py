"""Prune dbt manifest to canonical package only.

Canonical historically imported the inference repo as a dbt package,
leaving leftover model files that still get picked up by
`dbt docs generate`. This script strips everything except the
canonical project (package_name == 'dbt_project') so the lineage
graph at :8890 reflects only canonical models.
"""

import json
import pathlib

KEEP = "dbt_project"
SECTIONS = ("nodes", "sources", "exposures", "metrics", "tests", "macros", "semantic_models")
MAPS = ("parent_map", "child_map", "group_map")

manifest_path = pathlib.Path("target/manifest.json")
manifest = json.loads(manifest_path.read_text())

for section in SECTIONS:
    if section not in manifest:
        continue
    for key in list(manifest[section].keys()):
        if manifest[section][key].get("package_name") != KEEP:
            del manifest[section][key]

known = set(manifest.get("nodes", {}).keys()) | set(manifest.get("sources", {}).keys())
for map_name in MAPS:
    if map_name in manifest:
        manifest[map_name] = {
            k: [v for v in vs if v in known]
            for k, vs in manifest[map_name].items()
            if k in known
        }

manifest_path.write_text(json.dumps(manifest))
print(f"manifest pruned to {len(manifest['nodes'])} nodes, {len(manifest['sources'])} sources")
