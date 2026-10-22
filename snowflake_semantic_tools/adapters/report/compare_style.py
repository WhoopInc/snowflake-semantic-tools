"""The one stylesheet a comparison report inlines: light and dark themes, print, and chart classes.

Colours are custom properties set once per theme, so the inline SVG charts, which draw with the
classes below rather than fill attributes, follow the reader's colour scheme too. Nothing here
names a font file or a URL: the type is the system's own.
"""

from __future__ import annotations

STYLE = """
:root {
  color-scheme: light dark;
  --bg: #f6f7f9; --surface: #ffffff; --surface-2: #f1f3f6; --border: #e3e6eb;
  --text: #16191f; --muted: #5b6472; --faint: #8a93a1;
  --accent: #4f46e5; --accent-soft: #eef0ff; --accent-text: #3730a3; --on-accent: #ffffff;
  --pinned: #64748b; --pinned-soft: #eef1f5;
  --good: #15803d; --good-soft: #e8f6ed; --good-strong: #2fa35a;
  --bad: #b42318; --bad-soft: #fdecea; --bad-strong: #e5484d;
  --warn: #a15c07; --warn-soft: #fdf3e1;
  --mid-soft: #f3efe1;
  --shadow: 0 1px 2px rgba(16, 24, 40, .05), 0 1px 3px rgba(16, 24, 40, .06);
  --radius: 12px;
  --mono: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, "Liberation Mono", monospace;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #0e1116; --surface: #161a21; --surface-2: #1d222b; --border: #2a303b;
    --text: #e8ebf0; --muted: #a3acb9; --faint: #7d8796;
    --accent: #8b93ff; --accent-soft: #23264a; --accent-text: #c3c8ff; --on-accent: #0e1116;
    --pinned: #94a3b8; --pinned-soft: #222833;
    --good: #4ade80; --good-soft: #13291c; --good-strong: #22c55e;
    --bad: #ff8a80; --bad-soft: #351717; --bad-strong: #f05252;
    --warn: #fbbf24; --warn-soft: #33270f;
    --mid-soft: #2b2816;
    --shadow: 0 1px 2px rgba(0, 0, 0, .4);
  }
}
* { box-sizing: border-box; }
html { -webkit-text-size-adjust: 100%; }
body {
  margin: 0; background: var(--bg); color: var(--text);
  font: 15px/1.55 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
}
main { max-width: 1180px; margin: 0 auto; padding: 40px 28px 24px; }
h1, h2, h3, h4 { line-height: 1.25; margin: 0; letter-spacing: -.01em; }
h1 { font-size: 30px; font-weight: 700; }
h2 { font-size: 22px; font-weight: 650; }
h3 { font-size: 16px; font-weight: 650; margin-bottom: 12px; }
p { margin: 0; }
code, pre, .mono { font-family: var(--mono); font-size: 12.5px; }
.num, td.n, .big { font-variant-numeric: tabular-nums; }
.muted { color: var(--muted); }
.eyebrow { color: var(--accent); font-size: 12px; font-weight: 650; letter-spacing: .08em; text-transform: uppercase; }
.card {
  background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius);
  box-shadow: var(--shadow); padding: 20px 22px;
}
.page-head { display: flex; flex-direction: column; gap: 20px; margin-bottom: 28px; }
.page-head h1 { margin-top: 4px; }
.meta { display: grid; grid-template-columns: repeat(auto-fill, minmax(170px, 1fr)); gap: 12px 24px; margin: 0; }
.meta .wide { grid-column: span 2; }
.meta div { min-width: 0; }
.meta dt { color: var(--faint); font-size: 11.5px; font-weight: 600; letter-spacing: .05em; text-transform: uppercase; }
.meta dd { margin: 2px 0 0; overflow-wrap: break-word; }
.meta dd.mono { font-size: 13px; }
.page-head .meta {
  padding: 16px 20px; background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius);
}
.toc { display: flex; flex-wrap: wrap; gap: 8px; margin: -12px 0 28px; padding: 0; list-style: none; }
.toc a {
  color: var(--accent-text); background: var(--accent-soft); border-radius: 999px; padding: 4px 12px;
  text-decoration: none; font-size: 13px;
}
.eval { margin-top: 36px; }
.eval > header { display: flex; flex-direction: column; gap: 14px; margin-bottom: 18px; }
.title-row { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; }
.block { margin-top: 18px; }
.block > h3 { display: flex; align-items: baseline; gap: 10px; }
.block > h3 .muted { font-size: 13px; font-weight: 500; }
.badge {
  display: inline-flex; align-items: center; gap: 4px; border-radius: 999px; padding: 2px 9px;
  font-size: 12px; font-weight: 600; line-height: 1.5; border: 1px solid transparent; white-space: nowrap;
}
.b-kind { background: var(--accent-soft); color: var(--accent-text); }
.b-pinned { background: var(--pinned-soft); color: var(--pinned); border-color: var(--border); }
.b-candidate { background: var(--surface-2); color: var(--muted); border-color: var(--border); }
.b-ga { background: var(--good-soft); color: var(--good); }
.b-flag { background: var(--warn-soft); color: var(--warn); border-color: currentColor; }
.b-bad { background: var(--bad-soft); color: var(--bad); }
.b-rank { background: transparent; color: var(--faint); border-color: var(--border); }
.verdict {
  display: grid; grid-template-columns: auto 1fr; gap: 6px 16px; border-left: 5px solid var(--v);
  background: var(--surface);
}
.verdict .icon {
  grid-row: span 3; width: 44px; height: 44px; border-radius: 50%; display: grid; place-items: center;
  font-size: 22px; background: var(--v-soft); color: var(--v);
}
.verdict .label {
  color: var(--v); font-weight: 700; font-size: 13px; letter-spacing: .06em; text-transform: uppercase;
}
.verdict .summary { font-size: 18px; font-weight: 550; line-height: 1.45; }
.verdict .extra { display: flex; flex-direction: column; gap: 10px; margin-top: 6px; }
.v-winner { --v: var(--accent); --v-soft: var(--accent-soft); }
.v-pinned_holds { --v: var(--good); --v-soft: var(--good-soft); }
.v-no_clear_winner { --v: var(--warn); --v-soft: var(--warn-soft); }
.v-incomplete { --v: var(--bad); --v-soft: var(--bad-soft); }
.v-unknown { --v: var(--muted); --v-soft: var(--surface-2); }
pre.pin {
  margin: 0; padding: 12px 14px; background: var(--surface-2); border: 1px solid var(--border);
  border-radius: 8px; overflow-x: auto; user-select: all;
}
.hint { font-size: 13px; color: var(--muted); }
.excluded { margin: 0; padding-left: 18px; font-size: 13.5px; color: var(--muted); }
.cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(250px, 1fr)); gap: 14px; }
.scorecard { position: relative; overflow: hidden; display: flex; flex-direction: column; gap: 12px; }
.scorecard.is-pinned { border-top: 3px solid var(--pinned); }
.scorecard.is-winner { border-color: var(--accent); box-shadow: 0 0 0 1px var(--accent), var(--shadow); }
.ribbon {
  position: absolute; top: 14px; right: -34px; transform: rotate(45deg); width: 130px; text-align: center;
  background: var(--accent); color: var(--on-accent); font-size: 11px; font-weight: 700; letter-spacing: .06em;
  text-transform: uppercase; padding: 3px 0;
}
.scorecard h4 { font-size: 15.5px; padding-right: 54px; overflow-wrap: anywhere; }
.badges { display: flex; flex-wrap: wrap; gap: 6px; }
.big { font-size: 38px; font-weight: 700; letter-spacing: -.02em; line-height: 1; }
.headline .caption { font-size: 12.5px; color: var(--muted); margin-top: 6px; }
.stats {
  display: grid; grid-template-columns: 1fr 1fr; gap: 8px 14px; margin: 0; padding-top: 12px;
  border-top: 1px solid var(--border);
}
.stats dt {
  color: var(--faint); font-size: 11.5px; font-weight: 600; letter-spacing: .04em; text-transform: uppercase;
}
.stats dd { margin: 1px 0 0; font-variant-numeric: tabular-nums; font-weight: 600; }
.ident { font-family: var(--mono); font-size: 11.5px; color: var(--muted); overflow-wrap: anywhere; }
.status { font-size: 13px; border-radius: 8px; padding: 8px 10px; }
.status.s-partial { background: var(--warn-soft); color: var(--warn); }
.status.s-failed { background: var(--bad-soft); color: var(--bad); }
.status.s-unknown { background: var(--surface-2); color: var(--muted); }
.status .err { display: block; margin-top: 3px; font-family: var(--mono); font-size: 11.5px; overflow-wrap: anywhere; }
.pill {
  display: inline-block; border-radius: 999px; padding: 1px 8px; font-size: 11.5px; font-weight: 600;
  white-space: nowrap;
}
.pill-pending { background: var(--warn-soft); color: var(--warn); }
.pill-unavailable { background: var(--surface-2); color: var(--muted); border: 1px dashed var(--border); }
.charts { display: grid; grid-template-columns: 1fr; gap: 14px; }
.chart { margin: 0; }
.chart h4 { font-size: 14px; display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin-bottom: 8px; }
svg.plot { display: block; width: 100%; height: auto; overflow: visible; }
svg.plot text { fill: var(--text); font-size: 12px; font-family: inherit; }
svg.plot text.tick, svg.plot text.axis { fill: var(--muted); font-size: 11px; }
svg.plot text.value { font-variant-numeric: tabular-nums; font-weight: 600; }
svg.plot .track { fill: var(--surface-2); }
svg.plot .bar { fill: var(--accent); }
svg.plot .bar.pinned { fill: var(--pinned); }
svg.plot .whisker { stroke: var(--text); stroke-width: 1.5; opacity: .65; }
svg.plot .threshold { stroke: var(--bad-strong); stroke-width: 1.5; stroke-dasharray: 5 4; }
svg.plot text.threshold-label { fill: var(--bad); font-size: 11px; font-weight: 600; }
svg.plot .grid { stroke: var(--border); stroke-width: 1; }
svg.plot .axis-line { stroke: var(--faint); stroke-width: 1; }
svg.plot .point { fill: var(--accent); stroke: var(--surface); stroke-width: 2; }
svg.plot .point.pinned { fill: var(--pinned); }
.empty { padding: 28px; text-align: center; color: var(--muted); background: var(--surface-2); border-radius: 8px; }
.aside { margin: 10px 0 0; font-size: 13px; color: var(--muted); }
.table-wrap { overflow-x: auto; border: 1px solid var(--border); border-radius: 10px; }
table { border-collapse: collapse; width: 100%; font-size: 13.5px; }
caption { text-align: left; padding: 0 0 8px; color: var(--muted); font-size: 13px; }
th, td { padding: 9px 12px; border-bottom: 1px solid var(--border); text-align: left; vertical-align: top; }
thead th {
  background: var(--surface-2); color: var(--muted); font-size: 12px; font-weight: 650; letter-spacing: .03em;
  white-space: nowrap;
}
tbody tr:last-child > * { border-bottom: 0; }
tbody th { font-weight: 500; }
td.n, th.n { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
.qid { font-family: var(--mono); font-size: 11.5px; color: var(--faint); display: block; }
.qtext { display: block; min-width: 220px; max-width: 460px; }
.heat td.cell {
  text-align: center; white-space: nowrap; font-variant-numeric: tabular-nums; font-weight: 600; min-width: 92px;
  cursor: help;
}
.heat td.cell:hover { outline: 2px solid var(--accent); outline-offset: -2px; }
.c-pass { background: var(--good-soft); color: var(--good); }
.c-fail { background: var(--bad-soft); color: var(--bad); }
.c-score { background: var(--mid-soft); color: var(--warn); }
.c-none { color: var(--faint); font-weight: 400; }
.change { margin-left: 5px; font-size: 11px; }
.change.fixes { color: var(--good); }
.change.breaks { color: var(--bad); }
.legend { display: flex; flex-wrap: wrap; gap: 6px 18px; margin-top: 10px; font-size: 12.5px; color: var(--muted); }
.legend .swatch {
  display: inline-block; min-width: 26px; padding: 0 6px; margin-right: 6px; border-radius: 5px; text-align: center;
  font-weight: 600;
}
details.notes summary { cursor: pointer; font-weight: 650; }
details.notes[open] summary { margin-bottom: 12px; }
details.notes ul { margin: 0 0 12px; padding-left: 20px; }
details.notes li + li { margin-top: 4px; }
.runs { display: grid; grid-template-columns: max-content 1fr; gap: 6px 18px; margin: 0; }
.runs dt { font-weight: 600; }
.runs dd { margin: 0; }
.runs code { display: block; color: var(--muted); overflow-wrap: anywhere; }
footer.foot {
  max-width: 1180px; margin: 0 auto; padding: 18px 28px 40px; color: var(--faint); font-size: 12.5px; display: flex;
  justify-content: space-between; gap: 12px;
}
.sr { position: absolute; width: 1px; height: 1px; overflow: hidden; clip: rect(0 0 0 0); white-space: nowrap; }
@media (max-width: 640px) {
  main { padding: 24px 16px; }
  .verdict { grid-template-columns: 1fr; }
  .verdict .icon { grid-row: auto; }
}
@media print {
  :root {
    --bg: #fff; --surface: #fff; --surface-2: #f4f5f7; --border: #d0d5dd; --text: #000; --muted: #333; --faint: #555;
    --shadow: none;
  }
  body { background: #fff; font-size: 11pt; }
  main { max-width: none; padding: 0; }
  .card, .scorecard, .chart, .verdict, .table-wrap, tr {
    break-inside: avoid; page-break-inside: avoid; box-shadow: none;
  }
  .eval { break-before: page; }
  .eval:first-of-type { break-before: auto; }
  .toc { display: none; }
  .table-wrap { overflow: visible; }
  details.notes > *:not(summary) { display: block; }
  * { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
}
"""
