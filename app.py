# FlowUp v1 — Powering your growth
# Minimal Streamlit app using Google Sheets as storage (no SQL, no CSV)
# Features:
#  - Team login by name + PIN
#  - View balance, transfer to another team, see recent transactions
#  - Admin: create team, manual balance adjust, view all transactions
#
# Setup:
# 1) pip install streamlit pandas gspread gspread-dataframe passlib
# 2) In Streamlit Secrets (or .streamlit/secrets.toml) set:
#    gcp_service_account = """
#    { ...your Google service account JSON... }
#    """
#    gsheet_url = "https://docs.google.com/spreadsheets/d/<ID>"
# 3) Share the Sheet with the service account email as Editor.
# 4) streamlit run app.py

import json
from datetime import datetime
from typing import Optional

import pandas as pd
import streamlit as st
from passlib.hash import pbkdf2_sha256

# Optional libs for Google Sheets (must be installed)
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe

APP_TITLE = "FlowUp — Powering your growth"
SHEETS = {
    "teams": ["id", "name", "pin_hash", "balance"],
    "transactions": ["id", "timestamp", "from", "to", "amount", "note"],
    "settings": ["key", "value"],
}
DEFAULT_ADMIN_PIN = "0000"  # change via Admin → Settings (hash stored in settings sheet)

# ---------- Google Sheets helpers ----------

def _client():
    """Create gspread client from Streamlit secrets."""
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("Add gcp_service_account JSON to Streamlit secrets.")
    return gspread.service_account_from_dict(json.loads(st.secrets["gcp_service_account"]))


def _spreadsheet():
    if "gsheet_url" not in st.secrets:
        raise RuntimeError("Add gsheet_url to Streamlit secrets.")
    gc = _client()
    url_or_id = st.secrets["gsheet_url"]
    sid = url_or_id.split("/d/")[-1].split("/")[0] if "/d/" in url_or_id else url_or_id
    try:
        return gc.open_by_key(sid)
    except Exception:
        sh = gc.create("FlowUp_DB")
        return sh


def _ensure_ws(sh, title: str, columns: list[str]):
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows="1000", cols=str(max(10, len(columns) + 2)))
    values = ws.get_all_values()
    if not values:
        ws.update("A1", [columns])
    else:
        header = values[0]
        if header != columns:
            ws.clear()
            ws.update("A1", [columns])
    return ws


@st.cache_resource(show_spinner=False)
def _open_book():
    sh = _spreadsheet()
    # ensure all tabs exist
    for name, cols in SHEETS.items():
        _ensure_ws(sh, name, cols)
    return sh


@st.cache_data(show_spinner=False)
def get_df(name: str) -> pd.DataFrame:
    sh = _open_book()
    ws = _ensure_ws(sh, name, SHEETS[name])
    df = get_as_dataframe(ws, evaluate_formulas=False, header=0, dtype=str).fillna("")
    for c in SHEETS[name]:
        if c not in df.columns:
            df[c] = ""
    df = df[SHEETS[name]]
    # cast numeric columns
    if name == "teams":
        df["id"] = pd.to_numeric(df["id"], errors="coerce").fillna(0).astype(int)
        df["balance"] = pd.to_numeric(df["balance"], errors="coerce").fillna(0.0)
    elif name == "transactions":
        df["id"] = pd.to_numeric(df["id"], errors="coerce").fillna(0).astype(int)
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    return df


def save_df(name: str, df: pd.DataFrame):
    sh = _open_book()
    ws = _ensure_ws(sh, name, SHEETS[name])
    # write entire sheet for simplicity
    set_with_dataframe(ws, df[SHEETS[name]].fillna(""))
    get_df.clear()


def next_id(df: pd.DataFrame) -> int:
    if df.empty:
        return 1
    return int(pd.to_numeric(df["id"], errors="coerce").fillna(0).max()) + 1


# ---------- Settings (admin PIN) ----------

def _seed_settings():
    s = get_df("settings")
    if s.empty or not (s["key"] == "admin_pin_hash").any():
        hashed = pbkdf2_sha256.hash(DEFAULT_ADMIN_PIN)
        s = pd.DataFrame([["admin_pin_hash", hashed]], columns=["key", "value"])
        save_df("settings", s)


def get_admin_ok(pin: str) -> bool:
    s = get_df("settings")
    row = s[s["key"] == "admin_pin_hash"]
    if row.empty:
        _seed_settings()
        s = get_df("settings")
        row = s[s["key"] == "admin_pin_hash"]
    try:
        return pbkdf2_sha256.verify(pin, str(row.iloc[0]["value"]))
    except Exception:
        return False


def set_admin_pin(new_pin: str):
    s = get_df("settings")
    if (s["key"] == "admin_pin_hash").any():
        s.loc[s["key"] == "admin_pin_hash", "value"] = pbkdf2_sha256.hash(new_pin)
    else:
        s.loc[len(s)] = {"key": "admin_pin_hash", "value": pbkdf2_sha256.hash(new_pin)}
    save_df("settings", s)


# ---------- Teams & Transactions ----------

def create_team(name: str, pin: str):
    teams = get_df("teams")
    if (teams["name"].str.lower() == name.strip().lower()).any():
        raise ValueError("Team already exists")
    row = {
        "id": next_id(teams),
        "name": name.strip(),
        "pin_hash": pbkdf2_sha256.hash(pin),
        "balance": 0.0,
    }
    teams.loc[len(teams)] = row
    save_df("teams", teams)


def auth_team(name: str, pin: str) -> Optional[int]:
    teams = get_df("teams")
    t = teams[teams["name"].str.lower() == name.strip().lower()]
    if t.empty:
        return None
    if pbkdf2_sha256.verify(pin, str(t.iloc[0]["pin_hash"])):
        return int(t.iloc[0]["id"])
    return None


def adjust_balance(team_id: int, delta: float, note: str):
    teams = get_df("teams")
    mask = teams["id"].astype(int) == int(team_id)
    if not mask.any():
        raise ValueError("Team not found")
    teams.loc[mask, "balance"] = pd.to_numeric(teams.loc[mask, "balance"], errors="coerce").fillna(0) + float(delta)
    save_df("teams", teams)


def log_tx(fr: Optional[int], to: Optional[int], amount: float, note: str):
    tx = get_df("transactions")
    tx.loc[len(tx)] = {
        "id": next_id(tx),
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "from": int(fr) if fr else "",
        "to": int(to) if to else "",
        "amount": float(amount),
        "note": note,
    }
    save_df("transactions", tx)


# ---------- UI: Team ----------

def team_dashboard(team_id: int):
    st.subheader("Team Dashboard")
    teams = get_df("teams")
    trow = teams[teams["id"].astype(int) == int(team_id)].iloc[0]

    col_a, col_b = st.columns(2)
    with col_a:
        st.metric("Balance", f"{float(trow['balance']):.2f}")
    with col_b:
        st.write("")

    tabs = st.tabs(["Transfer", "History"])

    # Transfer tab
    with tabs[0]:
        others = teams[teams["id"].astype(int) != int(team_id)]["name"].tolist()
        if not others:
            st.info("No recipient teams yet.")
        else:
            target_name = st.selectbox("Send to", others)
            amount = st.number_input("Amount", min_value=0.0, value=0.0, step=1.0)
            note = st.text_input("Note")
            if st.button("Send"):
                balance = float(trow["balance"])
                if amount <= 0:
                    st.error("Enter positive amount")
                elif amount > balance:
                    st.error("Insufficient funds")
                else:
                    target_id = int(teams[teams["name"] == target_name].iloc[0]["id"])
                    adjust_balance(team_id, -amount, f"Transfer to {target_name}")
                    adjust_balance(target_id, amount, f"Transfer from {trow['name']}")
                    log_tx(team_id, target_id, amount, note or "transfer")
                    st.success("Transfer completed")
                    st.experimental_rerun()

    # History tab
    with tabs[1]:
        tx = get_df("transactions")
        filt = (tx["from"].astype(str) == str(team_id)) | (tx["to"].astype(str) == str(team_id))
        view = tx[filt].copy().sort_values("id", ascending=False).head(200)
        st.dataframe(view[["timestamp", "from", "to", "amount", "note"]], use_container_width=True)


# ---------- UI: Admin ----------

def admin_panel():
    st.subheader("Admin Console")
    tabs = st.tabs(["Teams", "Adjustments", "Transactions", "Settings"])  # minimal

    # Teams
    with tabs[0]:
        st.markdown("### Create team")
        name = st.text_input("Team name")
        pin = st.text_input("Team PIN", type="password")
        if st.button("Create"):
            if not name or not pin:
                st.error("Enter name and PIN")
            else:
                try:
                    create_team(name, pin)
                    st.success("Team created")
                    st.experimental_rerun()
                except Exception as e:
                    st.error(str(e))
        st.markdown("### All teams")
        st.dataframe(get_df("teams")[["id", "name", "balance"]], use_container_width=True)

    # Adjustments
    with tabs[1]:
        st.markdown("### Manual balance change")
        teams = get_df("teams")
        if teams.empty:
            st.info("No teams yet")
        else:
            rec = st.selectbox("Team", list(teams.to_dict("records")), format_func=lambda r: f"{r['name']} (bal {float(r['balance']):.2f})")
            delta = st.number_input("Delta (+ credit / - debit)", value=0.0, step=1.0)
            note = st.text_input("Note", value="admin adjust")
            if st.button("Apply"):
                adjust_balance(int(rec["id"]), float(delta), note)
                log_tx(None if delta >= 0 else int(rec["id"]), None if delta <= 0 else int(rec["id"]), abs(float(delta)), note)
                st.success("Updated")
                st.experimental_rerun()

    # Transactions
    with tabs[2]:
        st.markdown("### Transactions (latest 300)")
        tx = get_df("transactions").copy().sort_values("id", ascending=False).head(300)
        st.dataframe(tx, use_container_width=True)

    # Settings
    with tabs[3]:
        st.markdown("### Change Admin PIN")
        new_pin = st.text_input("New Admin PIN", type="password")
        if st.button("Update PIN"):
            if not new_pin:
                st.error("Enter a PIN")
            else:
                set_admin_pin(new_pin)
                st.success("Admin PIN updated")


# ---------- UI: Login ----------

def login_view():
    st.title(APP_TITLE)
    st.caption("School bank · minimal version (Sheets storage)")

    role = st.segmented_control("Login as", ["Team", "Admin"], default="Team")

    if role == "Team":
        teams = get_df("teams")
        names = ["(Select)"] + teams["name"].tolist()
        name = st.selectbox("Team", names)
        pin = st.text_input("PIN", type="password")
        if st.button("Enter"):
            if name == "(Select)":
                st.error("Choose your team")
            else:
                tid = auth_team(name, pin)
                if tid:
                    st.session_state["auth"] = {"role": "team", "team_id": tid}
                    st.experimental_rerun()
                else:
                    st.error("Invalid PIN")
    else:
        pin = st.text_input("Admin PIN", type="password")
        if st.button("Enter as admin"):
            if get_admin_ok(pin):
                st.session_state["auth"] = {"role": "admin"}
                st.experimental_rerun()
            else:
                st.error("Invalid admin PIN")


# ---------- App ----------

def main():
    st.set_page_config(page_title=APP_TITLE, page_icon="💸", layout="wide")
    _ = _open_book()  # ensure sheets & headers exist
    _seed_settings()  # ensure admin pin exists

    if "auth" not in st.session_state:
        login_view()
        return

    auth = st.session_state["auth"]
    if auth["role"] == "team":
        with st.sidebar:
            teams = get_df("teams")
            tinfo = teams[teams["id"].astype(int) == int(auth["team_id"])].iloc[0]
            st.header("👥 Team")
            st.write(str(tinfo["name"]))
            st.metric("Balance", f"{float(tinfo['balance']):.2f}")
            if st.button("Logout"):
                st.session_state.pop("auth")
                st.experimental_rerun()
        team_dashboard(int(auth["team_id"]))
    else:
        with st.sidebar:
            st.header("🛠️ Admin")
            if st.button("Logout"):
                st.session_state.pop("auth")
                st.experimental_rerun()
        admin_panel()


if __name__ == "__main__":
    main()
