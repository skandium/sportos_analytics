"""Streamlit HTML table rendering components."""

import hashlib
import html as html_mod
import urllib.parse

import pandas as pd
import streamlit as st


def _render_table(rows_data, columns, name_col="name", rank_col=True, link_col=None):
    """Render a nice HTML table with scoped CSS.

    rows_data: list of dicts (keys match columns)
    columns: list of column name strings to display
    name_col: which column gets runner links (None to disable)
    rank_col: whether to prepend a # column
    link_col: dict mapping column name -> row key containing the URL for that column
    """
    link_col = link_col or {}
    uid = "t" + hashlib.md5(str(id(rows_data)).encode()).hexdigest()[:8]
    time_cols = {c for c in columns if c in ("time", "best_time", "pace (min/km)", "aeg", "parim_aeg", "tempo (min/km)")}
    mono_cols = {c for c in columns if c.startswith("rank") or c.startswith("koht")} | time_cols

    css = f"""
<style>
#{uid} {{
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    font-size: 14px;
    border: 1px solid rgba(128,128,128,0.15);
    border-radius: 10px;
    overflow: hidden;
}}
#{uid} th {{
    padding: 12px 14px;
    text-align: left;
    font-weight: 600;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    color: rgba(128,128,128,0.7);
    background: rgba(128,128,128,0.06);
    border-bottom: 1px solid rgba(128,128,128,0.15);
    white-space: nowrap;
}}
#{uid} td {{
    padding: 10px 14px;
    border-bottom: 1px solid rgba(128,128,128,0.08);
    vertical-align: middle;
}}
#{uid} tr:last-child td {{
    border-bottom: none;
}}
#{uid} tbody tr:hover {{
    background: rgba(99,102,241,0.06);
}}
#{uid} .rk {{
    font-weight: 700;
    color: rgba(99,102,241,0.8);
    text-align: center;
    width: 36px;
}}
#{uid} .nm a {{
    text-decoration: none;
    font-weight: 500;
    color: inherit;
    transition: color 0.15s;
}}
#{uid} .nm a:hover {{
    color: #6366f1;
}}
#{uid} .mono {{
    font-variant-numeric: tabular-nums;
    font-family: 'SF Mono','Cascadia Code','Consolas',monospace;
    font-size: 13px;
}}
#{uid} .dim {{
    opacity: 0.45;
}}
</style>
"""

    header = ""
    if rank_col:
        header += '<th>#</th>'
    for c in columns:
        header += f'<th>{html_mod.escape(c)}</th>'

    body = []
    for idx, row in enumerate(rows_data):
        cells = []
        if rank_col:
            cells.append(f'<td class="rk">{idx + 1}</td>')
        for col in columns:
            raw = row.get(col, "")
            is_na = raw is None or (isinstance(raw, float) and pd.isna(raw))
            val = "" if is_na else html_mod.escape(str(raw))

            if col == name_col and not is_na:
                encoded = urllib.parse.quote(str(raw))
                cells.append(f'<td class="nm"><a href="?runner={encoded}" target="_self">{val}</a></td>')
            elif col in link_col and not is_na:
                href = row.get(link_col[col], "")
                if href:
                    cells.append(f'<td class="nm"><a href="{html_mod.escape(href)}" target="_blank">{val}</a></td>')
                else:
                    cells.append(f'<td>{val}</td>')
            elif col in mono_cols:
                cells.append(f'<td class="mono">{val}</td>')
            elif col in ("source", "allikas") and raw in ("predicted", "ennustus"):
                cells.append(f'<td class="dim">{val}</td>')
            else:
                cells.append(f'<td>{val}</td>')
        body.append(f'<tr>{"".join(cells)}</tr>')

    html = f"""{css}<table id="{uid}">
<thead><tr>{header}</tr></thead>
<tbody>{"".join(body)}</tbody>
</table>"""
    st.markdown(html, unsafe_allow_html=True)


def _styled_table(df, name_col="name", rank_col=True):
    """Render a pandas DataFrame as a styled table."""
    cols = list(df.columns)
    rows = [dict(row) for _, row in df.iterrows()]
    _render_table(rows, cols, name_col=name_col, rank_col=rank_col)


def _styled_pb_table(pb_data):
    """Render personal bests list-of-dicts as a styled table."""
    if not pb_data:
        return
    cols = [c for c in pb_data[0].keys() if not c.startswith("_")]
    _render_table(pb_data, cols, name_col=None, rank_col=False,
                  link_col={"võistlus": "_event_url"})
