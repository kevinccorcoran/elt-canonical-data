// Recolor the dbt docs lineage graph (Cytoscape) by SCHEMA instead of the
// stock resource-type coloring. Injected into target/index.html after
// `dbt docs generate` (see inject_schema_colors.py, wired into the docs
// container entrypoint). Pure client-side; no build step, safe to remove.
//
// Why it is written this way (verified against the shipped bundle):
//  - node data does NOT carry `schema`, so we map unique_id -> schema from the
//    served manifest.json ourselves.
//  - the Cytoscape instance is not global; we find it via the container's
//    `_cyreg` handle (a stable Cytoscape internal).
//  - the graph is built lazily when the Lineage tab opens and rebuilt on every
//    selection change, so we re-apply on an interval rather than once.
(function () {
  "use strict";
  if (window.__schemaColorInit) return;
  window.__schemaColorInit = true;

  // one color per schema (unchanged hues); covers both projects. Key ORDER =
  // legend order, set to lineage flow (upstream -> downstream) so the legend
  // reads top-to-bottom like the graph reads left-to-right: raw -> cdm ->
  // quality checks -> features -> clustering -> scoring -> validation ->
  // serving -> monitoring. Only schemas present in a given graph are shown.
  var COLORS = {
    raw: "#EDC948", cdm: "#4E79A7", data_quality: "#FF9DA7",
    quality: "#86BCB6", analysis: "#D37295",
    features: "#E15759", clustering: "#59A14F", scoring: "#B07AA1",
    validation: "#9C755F", serving: "#76B7B2", monitoring: "#F28E2B"
  };

  var schemaOf = {};          // unique_id and name -> schema
  var present = {};           // schemas actually in this graph

  function loadManifest() {
    return fetch("manifest.json", { cache: "no-store" })
      .then(function (r) { return r.json(); })
      .then(function (m) {
        var all = {}, k;
        for (k in (m.nodes || {})) all[k] = m.nodes[k];
        for (k in (m.sources || {})) all[k] = m.sources[k];
        Object.keys(all).forEach(function (id) {
          var n = all[id];
          var s = (n.config && n.config.schema) || n.schema;
          if (!s) return;
          schemaOf[id] = s;
          if (n.name) schemaOf[n.name] = s;
        });
      });
  }

  function findCy() {
    var els = document.getElementsByTagName("*");
    for (var i = 0; i < els.length; i++) {
      if (els[i]._cyreg && els[i]._cyreg.cy) return els[i]._cyreg.cy;
    }
    return null;
  }

  function schemaForNode(nd) {
    return schemaOf[nd.id()] || schemaOf[nd.data("unique_id")] ||
           schemaOf[nd.data("name")] || schemaOf[nd.data("label")];
  }

  function apply() {
    var cy = findCy();
    if (!cy) return;
    cy.batch(function () {
      cy.nodes().forEach(function (nd) {
        var s = schemaForNode(nd);
        var c = s && COLORS[s];
        if (c) {
          nd.style("background-color", c);
          nd.style("border-color", c);
          present[s] = true;
        }
      });
    });
    buildLegend();
  }

  function buildLegend() {
    if (document.getElementById("schema-color-legend")) return;
    if (!Object.keys(present).length) return;
    var box = document.createElement("div");
    box.id = "schema-color-legend";
    box.style.cssText =
      "position:fixed;right:12px;bottom:12px;z-index:99999;" +
      "background:rgba(255,255,255,0.96);border:1px solid #ccc;border-radius:6px;" +
      "padding:8px 10px;font:11px/1.4 Helvetica,Arial,sans-serif;color:#222;" +
      "box-shadow:0 1px 4px rgba(0,0,0,0.25);max-height:45vh;overflow:auto;";
    box.innerHTML = "<div style='font-weight:700;margin-bottom:4px'>schema</div>";
    Object.keys(COLORS).forEach(function (s) {
      if (!present[s]) return;
      var row = document.createElement("div");
      row.style.cssText = "display:flex;align-items:center;gap:6px;margin:2px 0;";
      row.innerHTML = "<span style='width:12px;height:12px;border-radius:3px;" +
        "display:inline-block;background:" + COLORS[s] + "'></span>" +
        "<span>" + s + "</span>";
      box.appendChild(row);
    });
    document.body.appendChild(box);
  }

  loadManifest().then(function () {
    apply();
    setInterval(apply, 1200);
  })["catch"](function (e) {
    console.warn("schema-color: manifest load failed", e);
  });
})();
