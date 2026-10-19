# AlphaStream dashboard - UI assembly (navbar, header, tabs). Part of app.R, split 2026-10-19: files load in name order (see app.R).
# ─── Define UI ───
ui <- navbarPage(
  title = "AlphaStream",
  id = "mainNav",   # active-tab input; gates the Lifecycle auto-refresh timer
  # the ONE shared connection, rendered on every tab above its content, plus the
  # global loading overlay (toggled by the shiny:busy/idle handler in head)
  header = tagList(
    connection_bar(),
    tags$div(id = "global-spinner",
      tags$div(id = "gs-track", tags$div(id = "gs-fill")),
      tags$div(id = "gs-label", "Loading..."))
  ),
  tags$head(
    tags$style(HTML(custom_css)),
    # Auto-heal Shiny's grey disconnect overlay. Forced reconnect stays OFF
    # (see server(), 2026-07-23: it replays sessions into the busy event loop
    # and segfaults), so a dropped websocket otherwise sits grey until a manual
    # reload. Instead: on a real drop, reload the tab after a short backoff, and
    # reset the counter once we reconnect. Caps at MAX tries so a server that is
    # genuinely down is not hammered (overlay is left for the user). MAX was 5,
    # which a run of dev restarts (each a disconnect) exhausted BEFORE the ~80s
    # boot finished reconnecting - stranding the tab on a stale frame while the
    # server ran fresh code. The ready() poll below already gates every reload on
    # a real 200, so a down server is never hammered regardless of MAX; the cap is
    # only a far backstop, so raise it to 40 to survive repeated restarts.
    tags$script(HTML("
      (function(){
        var KEY = 'wf_reconnectTries', MAX = 40;
        $(document).on('shiny:connected', function(){
          try { sessionStorage.removeItem(KEY); } catch (e) {}
        });
        $(document).on('shiny:disconnected', function(){
          var n = 0;
          try { n = parseInt(sessionStorage.getItem(KEY) || '0', 10); } catch (e) {}
          if (n >= MAX) return;               // likely down; stop reloading, leave overlay
          try { sessionStorage.setItem(KEY, n + 1); } catch (e) {}
          var delay = Math.min(1500 * Math.pow(1.6, n), 10000);
          // Reload only once the server actually answers 200. Reloading into a
          // still-booting / segfault-restarting process adds reconnect churn,
          // which is itself a segfault trigger - so poll readiness first.
          setTimeout(function ready(){
            fetch('/', { method: 'GET', cache: 'no-store' })
              .then(function(r){ if (r && r.ok) { location.reload(); } else { setTimeout(ready, 2000); } })
              .catch(function(){ setTimeout(ready, 2000); });
          }, delay);
        });
      })();
    ")),
    # Global estimated-progress bar: on any tab, whenever Shiny is busy (Connect,
    # Generate, heavy render). Real query progress isn't reported, so the fill
    # trickles asymptotically toward 90% over the expected load time and snaps to
    # 100% on idle. A 150ms delay skips the flash on trivial updates.
    tags$script(HTML("
      (function(){
        var startT = null, trickle = null, pct = 0, active = false;
        function ov(){ return document.getElementById('global-spinner'); }
        function paint(){
          var f = document.getElementById('gs-fill'); if(f) f.style.width = pct + '%';
          var l = document.getElementById('gs-label'); if(l) l.textContent = 'Loading... ' + Math.round(pct) + '%';
        }
        function start(){
          active = true; pct = 8; paint();
          var o = ov(); if(o) o.classList.add('gs-on');
          clearInterval(trickle);
          trickle = setInterval(function(){
            if(!active) return;
            pct += Math.max((90 - pct) * 0.06, 0.4);   // shrinking step -> approaches ~90%
            if(pct > 90) pct = 90;
            paint();
          }, 400);
        }
        function done(){
          active = false; clearInterval(trickle);
          pct = 100; paint();
          setTimeout(function(){ var o = ov(); if(o) o.classList.remove('gs-on'); pct = 0; paint(); }, 260);
        }
        $(document).on('shiny:busy', function(){ clearTimeout(startT); startT = setTimeout(start, 150); });
        $(document).on('shiny:idle', function(){
          clearTimeout(startT);
          var o = ov();
          if(active || (o && o.classList.contains('gs-on'))) done();
        });
      })();
    "))
  ),

  # ── Tab 1: Transition Range ──
  ui_tab_transition_range,
  ui_tab_data_qa,
  ui_tab_coverage,
  ui_tab_clusters,
  ui_tab_shortlist,
  ui_tab_id_flow,
  ui_tab_rank_stability,
  ui_tab_model_validation,
  ui_tab_predictions,
  ui_tab_forecast,
  ui_tab_lifecycle
)

