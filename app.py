"""Streamlit app for exploring Estonian running race results."""

import streamlit as st
import pandas as pd

from app_data import (
    get_connection, NM, DM, GOOD_RACE, DATE_12M,
    _min_time_for_distance, _clean_pb_sql,
    _ranked_runners, _rank_for_time,
    _build_runner_pbs,
)
from app_components import _render_table, _styled_table, _styled_pb_table


def page_overview():
    con = get_connection()
    st.info("Andmed on kogutud [sportos.eu](https://www.sportos.eu) tulemuste lehelt. ")
    events = con.execute("SELECT count(*) FROM events").fetchone()[0]
    distances = con.execute("SELECT count(*) FROM distances").fetchone()[0]
    results = con.execute("SELECT count(*) FROM results").fetchone()[0]
    runners = con.execute("SELECT count(DISTINCT name) FROM results").fetchone()[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Võistlused", f"{events:,}")
    c2.metric("Distantsid", f"{distances:,}")
    c3.metric("Tulemused", f"{results:,}")
    c4.metric("Sportlasi", f"{runners:,}")

    year_range = con.execute("SELECT min(date), max(date) FROM events").fetchone()
    st.caption(f"Andmed perioodist {year_range[0]} kuni {year_range[1]}")

    st.subheader("Tulemused aasta kaupa")
    df = con.execute("""
        SELECT year(e.date) AS aasta, count(r.id) AS tulemused
        FROM results r
        JOIN distances d ON r.distance_id = d.id
        JOIN events e ON e.id = d.event_id
        GROUP BY year(e.date)
        ORDER BY aasta
    """).fetchdf()
    st.bar_chart(df, x="aasta", y="tulemused")

    st.subheader("Populaarseimad distantsid")
    df = con.execute(f"""
        SELECT {DM} AS dm, count(r.id) AS tulemused
        FROM results r
        JOIN distances d ON r.distance_id = d.id
        WHERE d.distance_m IS NOT NULL {GOOD_RACE}
        GROUP BY dm
        ORDER BY tulemused DESC
        LIMIT 15
    """).fetchdf()
    df = df.sort_values("dm")
    df["distants"] = df["dm"].apply(
        lambda m: f"{m/1000:.1f} km" if m >= 1000 else f"{m} m"
    )
    import altair as alt
    order = df["distants"].tolist()
    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X("distants:N", sort=order, title="distants"),
        y=alt.Y("tulemused:Q", title="tulemused"),
    )
    st.altair_chart(chart, use_container_width=True)


def page_events():
    con = get_connection()
    st.subheader("Võistlused")
    search = st.text_input("Otsi võistlust nime järgi")

    if search:
        rows = con.execute("""
            SELECT e.name AS nimi, strftime(e.date, '%d/%m/%Y') AS kuupäev,
                   count(r.id) AS tulemused, e.url AS _event_url
            FROM events e
            LEFT JOIN distances d ON d.event_id = e.id
            LEFT JOIN results r ON r.distance_id = d.id
            WHERE e.name ILIKE ?
            GROUP BY e.id, e.name, e.date, e.url
            HAVING count(r.id) > 0
            ORDER BY e.date DESC
        """, [f"%{search}%"]).fetchall()
    else:
        rows = con.execute("""
            SELECT e.name AS nimi, strftime(e.date, '%d/%m/%Y') AS kuupäev,
                   count(r.id) AS tulemused, e.url AS _event_url
            FROM events e
            LEFT JOIN distances d ON d.event_id = e.id
            LEFT JOIN results r ON r.distance_id = d.id
            GROUP BY e.id, e.name, e.date, e.url
            HAVING count(r.id) > 0
            ORDER BY e.date DESC
        """).fetchall()

    display_rows = [{"nimi": r[0], "kuupäev": r[1], "tulemused": r[2], "_event_url": r[3]} for r in rows]
    _render_table(display_rows, ["nimi", "kuupäev", "tulemused"], name_col=None, rank_col=False,
                  link_col={"nimi": "_event_url"})


def page_percentile():
    con = get_connection()
    st.subheader("Protsentiili kalkulaator")
    st.caption("Vaata, kuidas sinu aeg paistab kõigi Eesti jooksjate seas")

    from scraper.predict import CANONICAL, CANONICAL_LABELS

    distance_options = {CANONICAL_LABELS[d]: d for d in CANONICAL}

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        dist_label = st.selectbox("Distants", list(distance_options.keys()), index=list(distance_options.keys()).index("5 km") if "5 km" in distance_options else 0)
    dist_m = distance_options[dist_label]

    with col2:
        hours = st.number_input("Tunnid", min_value=0, max_value=24, value=0)
    with col3:
        minutes = st.number_input("Minutid", min_value=0, max_value=59, value=25)
    with col4:
        seconds = st.number_input("Sekundid", min_value=0, max_value=59, value=0)

    time_s = hours * 3600 + minutes * 60 + seconds
    time_display = f"{hours}:{minutes:02d}:{seconds:02d}" if hours else f"{minutes}:{seconds:02d}"

    gender = st.radio("Sugu", ["Kõik", "M", "N"], horizontal=True)
    gender_val = gender if gender != "Kõik" else None

    runners = _ranked_runners(con, dist_m, gender=gender_val)
    total = len(runners)

    if total == 0:
        st.warning("Selle distantsi kohta tulemused puuduvad.")
        return

    rank, _ = _rank_for_time(runners, time_s)

    runners_recent = _ranked_runners(con, dist_m, gender=gender_val, date_where=DATE_12M)
    total_recent = len(runners_recent)
    rank_recent, _ = _rank_for_time(runners_recent, time_s)

    st.markdown("**Kogu aeg**")
    c1, c2, c3 = st.columns(3)
    c1.metric("Sinu aeg", time_display)
    c2.metric("Koht", f"{rank:,} / {total:,}")
    c3.metric("Jooksjaid kokku", f"{total:,}")

    st.markdown("**Viimased 12 kuud**")
    c1, c2, c3 = st.columns(3)
    c1.metric("Sinu aeg", time_display)
    c2.metric("Koht", f"{rank_recent:,} / {total_recent:,}" if total_recent > 0 else "Andmed puuduvad")
    c3.metric("Jooksjaid kokku", f"{total_recent:,}")

    st.subheader("Aegade jaotus")
    your_minute = time_s // 60
    gender_sql = f"AND r.gender = '{gender_val}'" if gender_val else ""
    df = con.execute(f"""
        SELECT
            (r.time_seconds / 60) AS minutid,
            count(*) AS jooksjaid
        FROM results r
        JOIN distances d ON r.distance_id = d.id
        WHERE {DM} = ? AND r.time_seconds IS NOT NULL {gender_sql} {GOOD_RACE}
        GROUP BY (r.time_seconds / 60)
        ORDER BY minutid
    """, [dist_m]).fetchdf()

    import altair as alt
    bars = alt.Chart(df).mark_bar().encode(
        x=alt.X("minutid:Q", title="Minutid", axis=alt.Axis(format="d")),
        y=alt.Y("jooksjaid:Q", title="Jooksjaid"),
    )
    rule = alt.Chart(pd.DataFrame({"minutid": [your_minute]})).mark_rule(
        color="red", strokeWidth=2
    ).encode(x="minutid:Q")
    label = alt.Chart(pd.DataFrame({"minutid": [your_minute], "label": ["Sina oled siin"]})).mark_text(
        align="left", dx=5, dy=-10, color="red", fontWeight="bold"
    ).encode(x="minutid:Q", text="label:N")
    st.altair_chart(bars + rule + label, use_container_width=True)


def page_top_runners():
    con = get_connection()
    st.subheader("Tippjooksjad")

    from scraper.predict import CANONICAL, CANONICAL_LABELS

    distance_options = {CANONICAL_LABELS[d]: d for d in CANONICAL}

    col1, col2 = st.columns(2)
    with col1:
        dist_label = st.selectbox("Distants", list(distance_options.keys()), key="top_dist")
    dist_m = distance_options[dist_label]

    with col2:
        gender = st.selectbox("Sugu", ["Kõik", "M", "N"], key="top_gender")

    last_12m = st.toggle("Viimased 12 kuud", value=False, key="top_12m")

    gender_val = gender if gender != "Kõik" else None
    min_time = _min_time_for_distance(dist_m)

    from scraper.predict import format_time

    date_where = DATE_12M if last_12m else ""
    runners = _ranked_runners(con, dist_m, gender=gender_val, date_where=date_where)

    top = runners[:50]

    if not top:
        st.info("Tulemused puuduvad.")
        return

    gender_where = f"AND r.gender = '{gender_val}'" if gender_val else ""
    actual_details = {}
    detail_rows = con.execute(f"""
        WITH pbs AS ({_clean_pb_sql(dist_m, gender_where + " " + date_where)})
        SELECT pbs.name, r.time_raw, r.category, e.name AS event,
               strftime(e.date, '%d/%m/%Y') AS date, e.url AS event_url
        FROM pbs
        JOIN results r ON {NM} = pbs.name AND r.time_seconds = pbs.clean_best
        JOIN distances d ON r.distance_id = d.id
        JOIN events e ON e.id = d.event_id
        WHERE {DM} = ? AND r.dnf = false {GOOD_RACE}
    """, [dist_m]).fetchall()
    for r_name, time_raw, category, event, date, event_url in detail_rows:
        if r_name not in actual_details:
            results_url = event_url.rstrip("/") + "/tulemused/" if event_url else ""
            actual_details[r_name] = (time_raw, category, event, date, results_url)

    display_rows = []
    for name, time_s, source in top:
        if name in actual_details:
            time_raw, category, event, date, results_url = actual_details[name]
            year = date[-4:] if date else ""
            event_str = f"{event} ({year})" if year else event
            display_rows.append({
                "nimi": name, "aeg": time_raw, "kategooria": category,
                "võistlus": event_str,
                "_event_url": results_url,
            })
        else:
            display_rows.append({
                "nimi": name, "aeg": format_time(time_s), "kategooria": "",
                "võistlus": "", "_event_url": "",
            })

    st.markdown("#### Kiireimad jooksjad")
    _render_table(display_rows, ["nimi", "aeg", "kategooria", "võistlus"],
                  name_col="nimi", link_col={"võistlus": "_event_url"})

    st.markdown("#### Sagedaseimad jooksjad")
    st.caption("Võistluste arvu järgi sellel distantsil (min 3)")
    df_freq = con.execute(f"""
        SELECT {NM} AS nimi, count(*) AS võistlusi, min(r.time_raw) AS parim_aeg
        FROM results r
        JOIN distances d ON r.distance_id = d.id
        WHERE {DM} = ? AND r.time_seconds >= {min_time} AND r.dnf = false AND {NM} != '' {gender_where} {GOOD_RACE}
        GROUP BY {NM}
        HAVING count(*) >= 3
        ORDER BY võistlusi DESC
        LIMIT 30
    """, [dist_m]).fetchdf()
    _styled_table(df_freq, name_col="nimi")


def page_runner_lookup():
    con = get_connection()
    st.subheader("Jooksja otsing")

    default_name = st.query_params.get("runner", "")
    name = st.text_input("Jooksja nimi (või osa nimest)", value=default_name)
    if not name or len(name) < 3:
        st.info("Sisesta vähemalt 3 tähemärki.")
        return

    df = con.execute(f"""
        SELECT {NM} AS nimi, r.time_raw AS aeg, r.category AS kategooria,
               CASE WHEN {DM} IS NOT NULL AND {DM} >= 1000 AND {DM} % 1000 = 0
                        THEN ({DM} / 1000)::VARCHAR || ' km'
                    WHEN {DM} IS NOT NULL AND {DM} >= 1000
                        THEN round({DM} / 1000.0, 2)::VARCHAR || ' km'
                    WHEN {DM} IS NOT NULL
                        THEN {DM}::VARCHAR || ' m'
                    ELSE d.label
               END AS distants,
               r.rank_overall AS koht, r.rank_category AS kat_koht,
               e.name AS võistlus, strftime(e.date, '%d/%m/%Y') AS kuupäev,
               e.url AS _event_url
        FROM results r
        JOIN distances d ON r.distance_id = d.id
        JOIN events e ON e.id = d.event_id
        WHERE (r.name ILIKE ? OR {NM} ILIKE ?)
        ORDER BY e.date DESC
    """, [f"%{name}%", f"%{name}%"]).fetchdf()

    if df.empty:
        st.warning("Tulemusi ei leitud.")
        return

    if len(df) > 1:
        runner_names = set(df["nimi"].unique())

        gender_row = con.execute(f"""
            SELECT r.gender FROM results r
            WHERE {NM} IN (SELECT unnest(?::VARCHAR[]))
              AND r.gender IS NOT NULL
            GROUP BY r.gender ORDER BY count(*) DESC LIMIT 1
        """, [list(runner_names)]).fetchone()
        runner_gender = gender_row[0] if gender_row else None

        # All-time PBs
        pb_data = _build_runner_pbs(con, runner_names, runner_gender)
        st.markdown("#### Isiklikud rekordid")
        _styled_pb_table(pb_data)

        # Last 12 months form
        pb_data_12m = _build_runner_pbs(con, runner_names, runner_gender, date_where=DATE_12M)
        st.markdown("#### Viimased 12 kuud")
        if pb_data_12m:
            _styled_pb_table(pb_data_12m)
        else:
            st.info("Viimase 12 kuu tulemusi ei leitud.")

    st.markdown("#### Kõik tulemused")
    st.caption(f"{len(df)} tulemust leitud")
    all_rows = []
    for _, row in df.iterrows():
        d = dict(row)
        url = d.pop("_event_url", "") or ""
        d["_event_url"] = url.rstrip("/") + "/tulemused/" if url else ""
        all_rows.append(d)
    visible_cols = [c for c in all_rows[0] if not c.startswith("_")]
    _render_table(all_rows, visible_cols, name_col="nimi", rank_col=False,
                  link_col={"võistlus": "_event_url"})


# --- Main ---
st.set_page_config(page_title="Eesti jooksutulemused", layout="wide")
st.title("Eesti jooksutulemused")

if st.query_params.get("runner"):
    default_page = "Jooksja otsing"
else:
    default_page = "Ülevaade"

pages = ["Ülevaade", "Võistlused", "Protsentiili kalkulaator", "Tippjooksjad", "Jooksja otsing"]
page = st.sidebar.radio("Leht", pages, index=pages.index(default_page))

if page == "Ülevaade":
    page_overview()
elif page == "Võistlused":
    page_events()
elif page == "Protsentiili kalkulaator":
    page_percentile()
elif page == "Tippjooksjad":
    page_top_runners()
elif page == "Jooksja otsing":
    page_runner_lookup()
