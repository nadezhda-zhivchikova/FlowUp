# FlowUp — Powering Your Growth (Sheets-first edition)
# Streamlit prototype for a school economic game bank — **Google Sheets as primary storage** (with CSV fallback)
# Single-file app: app.py
# --------------------------------------------------
# Features
# - Team login with PIN (no emails)
# - Balances and transfers
# - Loan applications (pending/approved/active/paid/defaulted)
# - Deposits (term-based) with interest on cycle close
# - Admin console: manage teams, approve loans, run end-of-cycle, adjust settings
# - **Ratings**: Stability Score leaderboard for teams
# - **Storage**: Google Sheets as the main database (per-worksheet tables). If not configured, falls back to local CSV.
# - **Exports**: CSV downloads + Google Sheets export (redundant when using Sheets storage)
#
# How to run locally:
#   1) pip install streamlit==1.37.1 pandas==2.2.2 passlib==1.7.4
#      # For Google Sheets storage:
#      # pip install gspread==6.0.2 gspread-dataframe==3.3.1
#   2) streamlit run app.py
#
# Configure on Streamlit Cloud:
#   In Deploy → Advanced settings → Secrets, add:
#   gcp_service_account = """
#   { ...your Google service account JSON... }
#   """
#   gsheet_url = "https://docs.google.com/spreadsheets/d/<ID>"
#   storage_mode = "sheets"   # optional (defaults to sheets here)
#
# Notes
# - This is a classroom-friendly prototype. No real money, no external auth.
# - Admin default PIN is "3141"; change it in the Admin > Settings panel.

import os
import io
import json
import datetime as dt
import threading
from typing import Dict, Optional

import streamlit as st
import pandas as pd
from passlib.hash import pbkdf2_sha256

# Optional Google Sheets libs (lazy import inside functions)
GSPREAD_AVAILABLE = True
try:
    import gspread  # type: ignore
    from gspread_dataframe import set_with_dataframe, get_as_dataframe  # type: ignore
except Exception:
    GSPREAD_AVAILABLE = False

# -------------------- CONSTANTS --------------------
DATA_DIR = "data"
FILES = {
    "settings": ["key", "value"],
    "teams": ["id", "name", "pin_hash", "balance", "trust_rating", "created_at"],
    "transactions": ["id", "ts", "from_team", "to_team", "amount", "type", "description"],
    "loans": ["id", "team_id", "principal", "interest_rate", "term_cycles", "cycles_elapsed", "status", "purpose", "created_at", "updated_at"],
    "deposits": ["id", "team_id", "amount", "interest_rate", "term_cycles", "cycles_elapsed", "status", "created_at", "updated_at"],
    "cycles": ["id", "label", "closed_at"],
}

DEFAULT_SETTINGS = {
    "admin_pin_hash": pbkdf2_sha256.hash("3141"),
    "max_loan_growth": "500",
    "rate_loan_growth": "0.10",
    "max_loan_micro": "200",
    "rate_loan_micro": "0.00",
    "term_loan_micro": "1",
    "term_loan_growth": "2",
    "penalty_late": "0.10",
    "deposit_rate": "0.05",
    "deposit_term": "2",
    # Storage & export settings
    "gsheet_url": "",
    "storage_mode": "sheets",  # default to Sheets-first
}

# Simple in-process locks per table to reduce race conditions
_LOCKS = {name: threading.Lock() for name in FILES.keys()}

# -------------------- UTILS --------------------

def now_iso():
    return dt.datetime.now().isoformat(timespec="seconds")


def data_path(name: str) -> str:
    return os.path.join(DATA_DIR, f"{name}.csv")


def ensure_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def df_cast_types(name: str, df: pd.DataFrame) -> pd.DataFrame:
    try:
        if name in ("teams", "transactions", "loans", "deposits", "cycles"):
            for col in df.columns:
                if col in ("id", "team_id", "from_team", "to_team", "term_cycles", "cycles_elapsed"):
                    if col in df:
                        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                if col in ("balance", "amount", "principal", "interest_rate"):
                    if col in df:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
    except Exception:
        pass
    return df

# -------------------- STORAGE MODE --------------------

def get_storage_mode() -> str:
    # priority: Streamlit secrets → settings.csv → DEFAULT
    if "storage_mode" in st.secrets:
        return str(st.secrets["storage_mode"]).strip().lower()
    try:
        return str(get_setting("storage_mode", DEFAULT_SETTINGS["storage_mode"]))
    except Exception:
        return DEFAULT_SETTINGS["storage_mode"]


def gspread_client_from_secrets_or_json(json_str: Optional[str] = None):
    if not GSPREAD_AVAILABLE:
        raise RuntimeError("gspread/gspread-dataframe not installed. Run: pip install gspread gspread-dataframe")
    if "gcp_service_account" in st.secrets:
        return gspread.service_account_from_dict(json.loads(st.secrets["gcp_service_account"]))
    if json_str:
        return gspread.service_account_from_dict(json.loads(json_str))
    raise RuntimeError("No Google service account provided (add to Streamlit Secrets or upload JSON).")


def get_gsheet_url_from_secrets_or_settings() -> str:
    if "gsheet_url" in st.secrets:
        return str(st.secrets["gsheet_url"]) or ""
    return str(get_setting("gsheet_url", "")) or ""


def extract_sheet_id(url_or_id: str) -> str:
    if "/d/" in url_or_id:
        try:
            return url_or_id.split("/d/")[1].split("/")[0]
        except Exception:
            return url_or_id
    return url_or_id

# ---- Sheets helpers ----
@st.cache_resource(show_spinner=False)
def _open_sheet_book() -> Optional[object]:
    try:
        gc = gspread_client_from_secrets_or_json(None)
        sid = extract_sheet_id(get_gsheet_url_from_secrets_or_settings())
        if not sid:
            return None
        try:
            return gc.open_by_key(sid)
        except Exception:
            # try create if missing
            sh = gc.create("EcoBank DB")
            return sh
    except Exception:
        return None


def _ensure_ws_columns(sh, title: str, columns: list[str]):
    try:
        try:
            ws = sh.worksheet(title)
        except Exception:
            ws = sh.add_worksheet(title=title, rows="1000", cols=str(max(10, len(columns)+2)))
        # If empty, write header row
        values = ws.get_all_values()
        if not values:
            ws.update("A1", [columns])
        else:
            # if header differs, overwrite header to our canonical columns
            header = values[0]
            if header != columns:
                ws.clear()
                ws.update("A1", [columns])
        return ws
    except Exception as e:
        raise RuntimeError(f"Cannot prepare worksheet '{title}': {e}")


# -------------------- LOAD & SAVE (Sheets-first) --------------------
@st.cache_data(show_spinner=False)
def get_df(name: str) -> pd.DataFrame:
    mode = get_storage_mode()
    cols = FILES[name]
    if mode == "sheets" and GSPREAD_AVAILABLE:
        sh = _open_sheet_book()
        if sh is not None:
            ws = _ensure_ws_columns(sh, name, cols)
            df = get_as_dataframe(ws, evaluate_formulas=False, header=0, dtype=str).fillna("")
            # Trim to defined columns and ensure ordering
            for c in cols:
                if c not in df.columns:
                    df[c] = ""
            df = df[cols]
            return df_cast_types(name, df)
        # if cannot open sheet, fall back to CSV silently
    # ---- CSV fallback ----
    ensure_dir()
    path = data_path(name)
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write(",".join(cols) + " ")
    with _LOCKS[name]:
        df = pd.read_csv(path, dtype=str).fillna("")
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df = df[cols]
    return df_cast_types(name, df)


def save_df(name: str, df: pd.DataFrame):
    cols = FILES[name]
    df = df[cols].copy()
    df = df.where(pd.notna(df), "")
    mode = get_storage_mode()
    if mode == "sheets" and GSPREAD_AVAILABLE:
        sh = _open_sheet_book()
        if sh is not None:
            with _LOCKS[name]:
                ws = _ensure_ws_columns(sh, name, cols)
                # Overwrite entirely for simplicity & consistency
                ws.clear()
                set_with_dataframe(ws, df)
                # Reapply header (set_with_dataframe writes headers by default)
            get_df.clear()
            return
    # ---- CSV fallback ----
    ensure_dir()
    tmp = data_path(name) + ".tmp"
    with _LOCKS[name]:
        df.to_csv(tmp, index=False, encoding="utf-8")
        os.replace(tmp, data_path(name))
    get_df.clear()


def refresh_all():
    get_df.clear()
    _open_sheet_book.clear()


def next_id(df: pd.DataFrame) -> int:
    if df.empty:
        return 1
    vals = pd.to_numeric(df["id"], errors="coerce").fillna(0).astype(int)
    return int(vals.max()) + 1


# -------------------- SETTINGS (built on storage) --------------------

def get_setting(key: str, default=None):
    s = load_settings()
    row = s[s["key"] == key]
    if not row.empty:
        return row.iloc[0]["value"]
    return default


def set_setting(key: str, value: str):
    s = load_settings().copy()
    if (s["key"] == key).any():
        s.loc[s["key"] == key, "value"] = str(value)
    else:
        s.loc[len(s)] = {"key": key, "value": str(value)}
    save_df("settings", s)


def load_settings() -> pd.DataFrame:
    s = _load_or_seed_settings()
    return s


def _load_or_seed_settings() -> pd.DataFrame:
    # Try to read; if empty, seed defaults
    try:
        s = _raw_load("settings")
        if s.empty:
            s = pd.DataFrame([(k, v) for k, v in DEFAULT_SETTINGS.items()], columns=["key", "value"])
            save_df("settings", s)
        return s
    except Exception:
        # If storage not ready yet (e.g., secrets missing): fallback to CSV
        ensure_dir()
        path = data_path("settings")
        if not os.path.exists(path):
            pd.DataFrame([(k, v) for k, v in DEFAULT_SETTINGS.items()], columns=["key", "value"]).to_csv(path, index=False)
        return pd.read_csv(path, dtype=str).fillna("")


def _raw_load(name: str) -> pd.DataFrame:
    # Bypass caching to avoid recursion in seeding
    cols = FILES[name]
    mode = get_storage_mode()
    if mode == "sheets" and GSPREAD_AVAILABLE:
        sh = _open_sheet_book()
        if sh is None:
            return pd.DataFrame(columns=cols)
        ws = _ensure_ws_columns(sh, name, cols)
        df = get_as_dataframe(ws, evaluate_formulas=False, header=0, dtype=str).fillna("")
        for c in cols:
            if c not in df.columns:
                df[c] = ""
        df = df[cols]
        return df
    # CSV fallback
    ensure_dir()
    path = data_path(name)
    if not os.path.exists(path):
        pd.DataFrame(columns=cols).to_csv(path, index=False)
    return pd.read_csv(path, dtype=str).fillna("")


# -------------------- INIT --------------------

def init_files():
    # Ensure all tables exist in the chosen storage
    for name, cols in FILES.items():
        df = get_df(name)  # this will create headers if needed
        if name == "settings" and df.empty:
            s = pd.DataFrame([(k, v) for k, v in DEFAULT_SETTINGS.items()], columns=["key", "value"])
            save_df("settings", s)
        if name == "cycles" and df.empty:
            cy = pd.DataFrame([[1, "Cycle 1", ""]], columns=FILES["cycles"])
            save_df("cycles", cy)


# -------------------- AUTH --------------------

def create_team(name: str, pin: str):
    teams = get_df("teams").copy()
    if (teams["name"].str.lower() == name.strip().lower()).any():
        raise ValueError("Team already exists")
    pin_hash = pbkdf2_sha256.hash(pin)
    tid = next_id(teams)
    row = {
        "id": tid,
        "name": name.strip(),
        "pin_hash": pin_hash,
        "balance": 0.0,
        "trust_rating": 0,
        "created_at": now_iso(),
    }
    teams.loc[len(teams)] = row
    save_df("teams", teams)


def auth_team(name: str, pin: str):
    teams = get_df("teams")
    row = teams[teams["name"].str.lower() == name.strip().lower()]
    if row.empty:
        return None
    pin_hash = str(row.iloc[0]["pin_hash"])
    if pbkdf2_sha256.verify(pin, pin_hash):
        return int(row.iloc[0]["id"])
    return None


def auth_admin(pin: str) -> bool:
    h = str(get_setting("admin_pin_hash", ""))
    try:
        return pbkdf2_sha256.verify(pin, h)
    except Exception:
        return False


# -------------------- TRANSACTIONS --------------------

def log_tx(ts, from_team, to_team, amount, t_type, desc):
    tx = get_df("transactions").copy()
    tx.loc[len(tx)] = {
        "id": next_id(tx),
        "ts": ts,
        "from_team": from_team if pd.notna(from_team) and from_team != "" else "",
        "to_team": to_team if pd.notna(to_team) and to_team != "" else "",
        "amount": float(amount),
        "type": t_type,
        "description": desc,
    }
    save_df("transactions", tx)


def adjust_balance(team_id: int, delta: float, t_type: str, desc: str):
    teams = get_df("teams").copy()
    mask = teams["id"].astype(int) == int(team_id)
    if not mask.any():
        raise ValueError("Team not found")
    teams.loc[mask, "balance"] = pd.to_numeric(teams.loc[mask, "balance"], errors="coerce").fillna(0) + float(delta)
    save_df("teams", teams)
    log_tx(now_iso(), team_id if delta < 0 else "", team_id if delta > 0 else "", abs(delta), t_type, desc)


# -------------------- RATINGS --------------------

def compute_stability_ratings() -> pd.DataFrame:
    """Compute a 0–100 Stability Score per team using simple, explainable factors.
    Factors (weights sum to 100):
      - Liquidity (balance vs median): 40
      - Debt pressure (active principal): 25 (lower debt → higher score)
      - Savings habit (locked deposits): 20
      - Discipline (overdue loans penalty): 15
    """
    teams = get_df("teams").copy()
    loans = get_df("loans").copy()
    deps = get_df("deposits").copy()

    if teams.empty:
        return pd.DataFrame(columns=["team_id","team","balance","active_debt","locked_deposits","overdue","score"])  # empty

    teams['balance'] = pd.to_numeric(teams['balance'], errors='coerce').fillna(0.0)

    # Active debt per team & overdue flag
    active = loans[loans['status'] == 'active'].copy()
    if not active.empty:
        active['principal'] = pd.to_numeric(active['principal'], errors='coerce').fillna(0.0)
        active['term_cycles'] = pd.to_numeric(active['term_cycles'], errors='coerce').fillna(0).astype(int)
        active['cycles_elapsed'] = pd.to_numeric(active['cycles_elapsed'], errors='coerce').fillna(0).astype(int)
        debt = active.groupby('team_id')['principal'].sum().rename('active_debt')
        overdue = active.assign(is_overdue = active['cycles_elapsed'] > active['term_cycles']) \
                       .groupby('team_id')['is_overdue'].max().rename('overdue')
    else:
        debt = pd.Series(dtype=float)
        overdue = pd.Series(dtype=bool)

    # Locked deposits per team
    locked = deps[deps['status'] == 'locked'].copy()
    if not locked.empty:
        locked['amount'] = pd.to_numeric(locked['amount'], errors='coerce').fillna(0.0)
        dep_sum = locked.groupby('team_id')['amount'].sum().rename('locked_deposits')
    else:
        dep_sum = pd.Series(dtype=float)

    df = teams[['id','name','balance']].rename(columns={'id':'team_id','name':'team'}).copy()
    df = df.merge(debt, left_on='team_id', right_index=True, how='left')
    df = df.merge(dep_sum, left_on='team_id', right_index=True, how='left')
    df = df.merge(overdue, left_on='team_id', right_index=True, how='left')
    df['active_debt'] = df['active_debt'].fillna(0.0)
    df['locked_deposits'] = df['locked_deposits'].fillna(0.0)
    df['overdue'] = df['overdue'].fillna(False)

    # Normalize with simple median scale for robustness
    bal_med = max(df['balance'].median(), 1.0)
    dep_med = max(df['locked_deposits'].median(), 1.0)
    debt_med = max(df['active_debt'].median(), 1.0)

    bal_score = (df['balance'] / bal_med).clip(0, 2) / 2  # 0..1, saturate at 2x median
    dep_score = (df['locked_deposits'] / dep_med).clip(0, 2) / 2
    debt_score = 1 - (df['active_debt'] / (2 * debt_med)).clip(0, 1)  # more debt → lower score
    discipline = (~df['overdue']).astype(float)  # 1 if not overdue, 0 if overdue

    df['score'] = (40*bal_score + 20*dep_score + 25*debt_score + 15*discipline)
    df['score'] = df['score'].round(1)
    return df.sort_values(['score','team'], ascending=[False, True]).reset_index(drop=True)


# -------------------- EXPORTS --------------------

def export_csv_bytes(name: str) -> bytes:
    df = get_df(name).copy()
    return df.to_csv(index=False).encode('utf-8')


def export_all_csv_zip() -> bytes:
    import zipfile
    from io import BytesIO
    mem = BytesIO()
    with zipfile.ZipFile(mem, mode='w', compression=zipfile.ZIP_DEFLATED) as z:
        for name in FILES.keys():
            z.writestr(f"{name}.csv", get_df(name).to_csv(index=False))
        ratings = compute_stability_ratings()
        z.writestr("ratings.csv", ratings.to_csv(index=False))
    mem.seek(0)
    return mem.read()


def export_to_gsheets(json_str: Optional[str], sheet_url_or_id: str) -> str:
    # For Sheets-first mode this simply ensures all tabs are present & refreshed
    gc = gspread_client_from_secrets_or_json(json_str)
    sid = extract_sheet_id(sheet_url_or_id.strip())
    try:
        sh = gc.open_by_key(sid)
    except Exception:
        sh = gc.create("EcoBank Export")
        sid = sh.id
    datasets: Dict[str, pd.DataFrame] = {**{k: get_df(k).copy() for k in FILES.keys()}, "ratings": compute_stability_ratings()}
    for title, df in datasets.items():
        try:
            ws = sh.worksheet(title)
            sh.del_worksheet(ws)
        except Exception:
            pass
        ws = sh.add_worksheet(title=title, rows=str(max(1000, len(df)+10)), cols=str(max(10, len(df.columns)+2)))
        set_with_dataframe(ws, df)
    return f"https://docs.google.com/spreadsheets/d/{sid}"


# -------------------- TEAM VIEWS --------------------

def team_dashboard(team_id: int):
    st.subheader("Team Dashboard")
    teams = get_df("teams")
    trow = teams[teams["id"].astype(int) == int(team_id)].iloc[0]
    st.metric("Current Balance", f"{float(trow['balance']):.2f}")
    st.caption(f"Trust rating: {trow['trust_rating']}")

    tabs = st.tabs(["Transfers", "Loans", "Deposits", "History", "Leaderboard"])

    # Transfers
    with tabs[0]:
        st.markdown("### Transfer to another team")
        others = teams[teams["id"].astype(int) != int(team_id)]["name"].tolist()
        if not others:
            st.info("No other teams yet.")
        else:
            target_name = st.selectbox("Recipient team", others)
            amount = st.number_input("Amount", min_value=0.0, value=0.0, step=1.0)
            note = st.text_input("Note (what is this payment for?)")
            if st.button("Send payment", use_container_width=True):
                balance = float(trow["balance"])
                if amount <= 0:
                    st.error("Enter a positive amount.")
                elif amount > balance:
                    st.error("Insufficient funds.")
                else:
                    target_id = int(teams[teams["name"] == target_name].iloc[0]["id"])
                    adjust_balance(team_id, -amount, "transfer", note)
                    adjust_balance(target_id, amount, "transfer", f"Incoming: {note}")
                    st.success("Transfer sent.")
                    st.experimental_rerun()

    # Loans
    with tabs[1]:
        st.markdown("### Loans")
        sub = st.radio("Loan actions", ["Apply", "Repay", "My loans"], horizontal=True)
        loans = get_df("loans")
        if sub == "Apply":
            purpose = st.text_area("Purpose (why do you need this loan?)")
            product = st.selectbox("Product", ["MicroLoan (<=200, 0% for 1 cycle)", "GrowthLoan (<=500, 10% for 2 cycles)"])
            if "Micro" in product:
                cap = float(get_setting("max_loan_micro", "200"))
                rate = float(get_setting("rate_loan_micro", "0.00"))
                term = int(get_setting("term_loan_micro", "1"))
            else:
                cap = float(get_setting("max_loan_growth", "500"))
                rate = float(get_setting("rate_loan_growth", "0.10"))
                term = int(get_setting("term_loan_growth", "2"))
            amount = st.number_input(f"Amount (<= {cap})", min_value=0.0, max_value=cap, step=10.0)
            if st.button("Submit application", type="primary"):
                loans = get_df("loans").copy()
                loans.loc[len(loans)] = {
                    "id": next_id(loans),
                    "team_id": int(team_id),
                    "principal": float(amount),
                    "interest_rate": float(rate),
                    "term_cycles": int(term),
                    "cycles_elapsed": 0,
                    "status": "pending",
                    "purpose": purpose,
                    "created_at": now_iso(),
                    "updated_at": "",
                }
                save_df("loans", loans)
                st.success("Application submitted. Bank will review it soon.")
        elif sub == "Repay":
            my = loans[(loans["team_id"].astype(str) == str(team_id)) & (loans["status"].isin(["active", "approved"]))]
            if my.empty:
                st.info("No active loans.")
            else:
                def fmt(r):
                    return f"Loan #{int(r['id'])} | Remain: {float(r['principal']):.2f} @ {float(r['interest_rate'])*100:.0f}%"
                row = st.selectbox("Select loan", list(my.to_dict("records")), format_func=fmt)
                repay_amt = st.number_input("Repayment amount", min_value=0.0, value=0.0, step=10.0)
                if st.button("Repay now"):
                    teams = get_df("teams")
                    bal = float(teams[teams["id"].astype(int) == int(team_id)].iloc[0]["balance"])
                    if repay_amt <= 0:
                        st.error("Enter a positive amount.")
                    elif repay_amt > bal:
                        st.error("Insufficient funds.")
                    else:
                        loans = get_df("loans").copy()
                        mask = loans["id"].astype(int) == int(row["id"])
                        new_pr = max(0.0, float(loans.loc[mask, "principal"].iloc[0]) - float(repay_amt))
                        loans.loc[mask, "principal"] = new_pr
                        loans.loc[mask, "updated_at"] = now_iso()
                        if new_pr <= 1e-6:
                            loans.loc[mask, "status"] = "paid"
                        save_df("loans", loans)
                        adjust_balance(team_id, -repay_amt, "loan_repayment", f"Loan #{int(row['id'])} repayment")
                        st.success("Repayment processed.")
                        st.experimental_rerun()
        else:
            my_loans = loans[loans["team_id"].astype(int) == int(team_id)].copy()
            my_loans = my_loans.rename(columns={
                "id": "LoanID",
                "principal": "Remaining",
                "interest_rate": "Rate",
                "term_cycles": "Term",
                "cycles_elapsed": "Elapsed",
                "status": "Status",
                "created_at": "Created",
            })
            st.dataframe(my_loans[["LoanID", "Remaining", "Rate", "Term", "Elapsed", "Status", "Created"]], use_container_width=True)

    # Deposits
    with tabs[2]:
        st.markdown("### Deposits")
        subd = st.radio("Deposit actions", ["Open", "My deposits"], horizontal=True)
        if subd == "Open":
            rate = float(get_setting("deposit_rate", "0.05"))
            term = int(get_setting("deposit_term", "2"))
            amt = st.number_input("Amount to lock", min_value=0.0, value=0.0, step=10.0)
            if st.button("Lock deposit"):
                teams = get_df("teams")
                bal = float(teams[teams["id"].astype(int) == int(team_id)].iloc[0]["balance"])
                if amt <= 0:
                    st.error("Enter a positive amount.")
                elif amt > bal:
                    st.error("Insufficient funds.")
                else:
                    deps = get_df("deposits").copy()
                    deps.loc[len(deps)] = {
                        "id": next_id(deps),
                        "team_id": int(team_id),
                        "amount": float(amt),
                        "interest_rate": float(rate),
                        "term_cycles": int(term),
                        "cycles_elapsed": 0,
                        "status": "locked",
                        "created_at": now_iso(),
                        "updated_at": "",
                    }
                    save_df("deposits", deps)
                    adjust_balance(team_id, -amt, "deposit_lock", f"Locked {amt:.2f} for {term} cycles @ {rate*100:.0f}%")
                    st.success("Deposit locked.")
                    st.experimental_rerun()
        else:
            deps = get_df("deposits")
            mine = deps[deps["team_id"].astype(int) == int(team_id)].copy()
            mine = mine.rename(columns={
                "id": "DepositID",
                "amount": "Amount",
                "interest_rate": "Rate",
                "term_cycles": "Term",
                "cycles_elapsed": "Elapsed",
                "status": "Status",
            })
            st.dataframe(mine[["DepositID", "Amount", "Rate", "Term", "Elapsed", "Status"]], use_container_width=True)

    # History
    with tabs[3]:
        st.markdown("### Recent transactions")
        tx = get_df("transactions")
        filt = (tx["from_team"].astype(str) == str(team_id)) | (tx["to_team"].astype(str) == str(team_id))
        view = tx[filt].copy().sort_values("id", ascending=False).head(200)
        st.dataframe(view[["ts", "type", "amount", "description"]], use_container_width=True)

    # Leaderboard
    with tabs[4]:
        st.markdown("### Stability Leaderboard")
        ratings = compute_stability_ratings()
        top = ratings[["team","score","balance","active_debt","locked_deposits","overdue"]]
        st.dataframe(top, use_container_width=True)


# -------------------- ADMIN VIEWS --------------------

def admin_console():
    st.subheader("Admin Console")
    tabs = st.tabs(["Teams", "Loans", "Deposits", "Cycles & Interest", "Settings", "Ledger", "Ratings & Export"]) 

    # Teams
    with tabs[0]:
        st.markdown("### Manage teams")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Create team**")
            name = st.text_input("Team name")
            pin = st.text_input("Team PIN", type="password")
            if st.button("Create team"):
                if not name or not pin:
                    st.error("Enter name and PIN.")
                else:
                    try:
                        create_team(name, pin)
                        st.success("Team created.")
                        st.experimental_rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")
        with col2:
            st.markdown("**Adjust balance (manual)**")
            teams = get_df("teams")
            if teams.empty:
                st.info("No teams yet.")
            else:
                options = list(teams.to_dict("records"))
                team_row = st.selectbox("Team", options, format_func=lambda r: f"{r['name']} (bal {float(r['balance']):.2f})")
                delta = st.number_input("Delta (positive = credit, negative = debit)", value=0.0, step=10.0)
                note = st.text_input("Reason")
                if st.button("Apply adjustment"):
                    adjust_balance(int(team_row["id"]), float(delta), "admin_adjust", note or "Manual adjust")
                    st.success("Balance updated.")
                    st.experimental_rerun()
        st.divider()
        st.markdown("**All teams**")
        st.dataframe(get_df("teams")[['id','name','balance','trust_rating','created_at']], use_container_width=True)

    # Loans
    with tabs[1]:
        st.markdown("### Loan approvals & management")
        loans = get_df("loans")
        teams = get_df("teams")
        pending = loans[loans["status"] == "pending"].copy()
        if pending.empty:
            st.info("No pending applications.")
        else:
            show = pending.copy()
            show = show.merge(teams[["id", "name"]], left_on="team_id", right_on="id", how="left", suffixes=("","_team"))
            show = show.rename(columns={"name": "team"})
            st.dataframe(show[["id","team","principal","interest_rate","term_cycles","purpose","status","created_at"]], use_container_width=True)

            sel = st.selectbox("Select application", list(pending.to_dict("records")), format_func=lambda r: f"#{int(r['id'])} | Team #{int(r['team_id'])} | {float(r['principal']):.2f} @ {float(r['interest_rate'])*100:.0f}% for {int(r['term_cycles'])}")
            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("Approve & disburse", type="primary"):
                    loans = get_df("loans").copy()
                    mask = loans["id"].astype(int) == int(sel["id"])
                    loans.loc[mask, "status"] = "active"
                    loans.loc[mask, "updated_at"] = now_iso()
                    save_df("loans", loans)
                    adjust_balance(int(sel["team_id"]), float(sel["principal"]), "loan_disbursement", f"Loan #{int(sel['id'])} disbursement")
                    st.success("Approved & disbursed.")
                    st.experimental_rerun()
            with c2:
                if st.button("Reject"):
                    loans = get_df("loans").copy()
                    mask = loans["id"].astype(int) == int(sel["id"])
                    loans.loc[mask, "status"] = "rejected"
                    loans.loc[mask, "updated_at"] = now_iso()
                    save_df("loans", loans)
                    st.warning("Application rejected.")
                    st.experimental_rerun()
            with c3:
                if st.button("Delete app"):
                    loans = get_df("loans").copy()
                    loans = loans[loans["id"].astype(int) != int(sel["id"])]
                    save_df("loans", loans)
                    st.info("Deleted.")
                    st.experimental_rerun()
        st.divider()
        st.markdown("**Active loans**")
        active = get_df("loans")
        active = active[active["status"].isin(["approved","active"])].copy()
        if not active.empty:
            st.dataframe(active[["id","team_id","principal","interest_rate","term_cycles","cycles_elapsed","status"]], use_container_width=True)
        else:
            st.info("No active loans.")

    # Deposits
    with tabs[2]:
        st.markdown("### Deposits overview")
        deps = get_df("deposits")
        teams = get_df("teams")
        if not deps.empty:
            show = deps.merge(teams[["id","name"]], left_on="team_id", right_on="id", how="left")
            show = show.rename(columns={"name":"team"})
            st.dataframe(show[["id","team","amount","interest_rate","term_cycles","cycles_elapsed","status"]], use_container_width=True)
        else:
            st.info("No deposits yet.")

    # Cycles & Interest Processing
    with tabs[3]:
        st.markdown("### End-of-cycle processing")
        st.caption("Run at the end of each game cycle to accrue interest, penalties, and advance terms.")
        if st.button("Run end-of-cycle processing", type="primary"):
            end_of_cycle()
            st.success("Cycle processed.")
        st.markdown("**Cycles**")
        cy = get_df("cycles")
        st.dataframe(cy.sort_values("id", ascending=False), use_container_width=True)
        last_id = int(cy["id"].max()) if not cy.empty else 1
        new_label = st.text_input("New cycle label", value=f"Cycle {last_id+1}")
        if st.button("Start new cycle"):
            cy = get_df("cycles").copy()
            open_mask = cy["closed_at"].astype(str).fillna("") == ""
            if open_mask.any():
                cy.loc[open_mask, "closed_at"] = now_iso()
            cy.loc[len(cy)] = {"id": next_id(cy), "label": new_label, "closed_at": ""}
            save_df("cycles", cy)
            st.success("New cycle started.")
            st.experimental_rerun()

    # Settings
    with tabs[4]:
        st.markdown("### Settings")
        st.caption("Adjust interest rates, caps, and admin PIN.")
        col1, col2, col3 = st.columns(3)
        with col1:
            max_micro = st.number_input("Max MicroLoan", value=float(get_setting("max_loan_micro", "200")), step=10.0)
            rate_micro = st.number_input("Rate MicroLoan", value=float(get_setting("rate_loan_micro", "0.0")), step=0.01, format="%.2f")
            term_micro = st.number_input("Term MicroLoan (cycles)", value=int(get_setting("term_loan_micro", "1")), step=1)
        with col2:
            max_growth = st.number_input("Max GrowthLoan", value=float(get_setting("max_loan_growth", "500")), step=10.0)
            rate_growth = st.number_input("Rate GrowthLoan", value=float(get_setting("rate_loan_growth", "0.10")), step=0.01, format="%.2f")
            term_growth = st.number_input("Term GrowthLoan (cycles)", value=int(get_setting("term_loan_growth", "2")), step=1)
        with col3:
            deposit_rate = st.number_input("Deposit rate per term", value=float(get_setting("deposit_rate", "0.05")), step=0.01, format="%.2f")
            deposit_term = st.number_input("Deposit term (cycles)", value=int(get_setting("deposit_term", "2")), step=1)
            penalty_late = st.number_input("Penalty for late (on remaining)", value=float(get_setting("penalty_late", "0.10")), step=0.01, format="%.2f")
        if st.button("Save settings"):
            set_setting("max_loan_micro", str(max_micro))
            set_setting("rate_loan_micro", str(rate_micro))
            set_setting("term_loan_micro", str(term_micro))
            set_setting("max_loan_growth", str(max_growth))
            set_setting("rate_loan_growth", str(rate_growth))
            set_setting("term_loan_growth", str(term_growth))
            set_setting("deposit_rate", str(deposit_rate))
            set_setting("deposit_term", str(deposit_term))
            set_setting("penalty_late", str(penalty_late))
            st.success("Settings saved.")
        st.divider()
        st.markdown("**Change Admin PIN**")
        new_pin = st.text_input("New admin PIN", type="password")
        if st.button("Update admin PIN"):
            if not new_pin:
                st.error("Enter a PIN.")
            else:
                set_setting("admin_pin_hash", pbkdf2_sha256.hash(new_pin))
                st.success("Admin PIN updated.")

    # Ledger
    with tabs[5]:
        st.markdown("### Transaction ledger")
        led = get_df("transactions").copy().sort_values("id", ascending=False).head(500)
        teams = get_df("teams")[['id','name']].copy()
        teams['id'] = teams['id'].astype(str)
        led['from_team'] = led['from_team'].astype(str)
        led['to_team'] = led['to_team'].astype(str)
        led = led.merge(teams, left_on='from_team', right_on='id', how='left', suffixes=("","_from"))
        led = led.rename(columns={"name":"from_name"})
        led = led.merge(teams, left_on='to_team', right_on='id', how='left', suffixes=("","_to"))
        led = led.rename(columns={"name":"to_name"})
        view = led[["id_x","ts","from_name","to_name","amount","type","description"]].rename(columns={"id_x":"id"})
        st.dataframe(view, use_container_width=True)

    # Ratings & Export
    with tabs[6]:
        st.markdown("### Stability Ratings")
        ratings = compute_stability_ratings()
        st.dataframe(ratings[["team","score","balance","active_debt","locked_deposits","overdue"]], use_container_width=True)

        st.markdown("### Export data")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.download_button("Download ALL as ZIP (CSV + ratings)", data=export_all_csv_zip(), file_name="ecobank_export.zip", mime="application/zip", use_container_width=True)
        with c2:
            st.download_button("teams.csv", data=export_csv_bytes("teams"), file_name="teams.csv", mime="text/csv", use_container_width=True)
            st.download_button("transactions.csv", data=export_csv_bytes("transactions"), file_name="transactions.csv", mime="text/csv", use_container_width=True)
        with c3:
            st.download_button("loans.csv", data=export_csv_bytes("loans"), file_name="loans.csv", mime="text/csv", use_container_width=True)
            st.download_button("deposits.csv", data=export_csv_bytes("deposits"), file_name="deposits.csv", mime="text/csv", use_container_width=True)

        st.divider()
        st.markdown("#### Google Sheets export (refresh)")
        gsheet_url = st.text_input("Google Sheet URL or ID", value=get_gsheet_url_from_secrets_or_settings())
        cred_file = st.file_uploader("Upload Google Service Account JSON (optional)", type=["json"])
        if st.button("Sync to Google Sheets now"):
            try:
                cred_json = cred_file.read().decode("utf-8") if cred_file else None
                url = export_to_gsheets(cred_json, gsheet_url)
                set_setting("gsheet_url", gsheet_url)
                st.success(f"Synced to Google Sheets. Open: {url}")
            except Exception as e:
                st.error(f"Sync failed: {e}")


# -------------------- PROCESSING --------------------

def end_of_cycle():
    penalty = float(get_setting("penalty_late", "0.10"))
    now = now_iso()

    # LOANS: accrue interest and advance cycle
    loans = get_df("loans").copy()
    changed = False
    for idx, row in loans.iterrows():
        if row["status"] != "active":
            continue
        principal = float(row["principal"]) if str(row["principal"]).strip() != "" else 0.0
        rate = float(row["interest_rate"]) if str(row["interest_rate"]).strip() != "" else 0.0
        term = int(row["term_cycles"]) if str(row["term_cycles"]).strip() != "" else 0
        elapsed = int(row["cycles_elapsed"]) + 1
        # interest accrual on remaining principal
        interest_amt = principal * rate
        principal += interest_amt
        # late penalty
        if elapsed > term and term > 0:
            principal += principal * float(penalty)
            if elapsed >= term + 2:
                loans.at[idx, "status"] = "defaulted"
        loans.at[idx, "principal"] = principal
        loans.at[idx, "cycles_elapsed"] = elapsed
        loans.at[idx, "updated_at"] = now
        changed = True
    if changed:
        save_df("loans", loans)

    # DEPOSITS: advance cycle and release if matured
    deps = get_df("deposits").copy()
    teams = get_df("teams").copy()
    d_changed = False
    for idx, row in deps.iterrows():
        if row["status"] not in ("locked", "matured"):
            continue
        elapsed = int(row["cycles_elapsed"]) + 1
        deps.at[idx, "cycles_elapsed"] = elapsed
        deps.at[idx, "updated_at"] = now
        if elapsed >= int(row["term_cycles"]) and row["status"] == "locked":
            payout = float(row["amount"]) * (1.0 + float(row["interest_rate"]))
            deps.at[idx, "status"] = "released"
            tid = int(row["team_id"])
            mask = teams["id"].astype(int) == tid
            teams.loc[mask, "balance"] = pd.to_numeric(teams.loc[mask, "balance"], errors="coerce").fillna(0) + payout
            tx = get_df("transactions").copy()
            tx.loc[len(tx)] = {
                "id": next_id(tx),
                "ts": now,
                "from_team": "",
                "to_team": tid,
                "amount": payout,
                "type": "deposit_release",
                "description": f"Deposit #{int(row['id'])} released",
            }
            save_df("transactions", tx)
            d_changed = True
    if d_changed:
        save_df("deposits", deps)
        save_df("teams", teams)


# -------------------- UI --------------------

def login_view():
    st.title("EcoBank — powering your growth")
    st.caption("School economic game banking prototype (Sheets-first)")
    st.link_button("View on GitHub", "https://github.com/<user>/<repo>", use_container_width=True)

    # Storage indicator
    mode = get_storage_mode()
    if mode == "sheets" and GSPREAD_AVAILABLE and _open_sheet_book() is not None:
        st.success("Storage: Google Sheets")
    elif mode == "sheets" and not GSPREAD_AVAILABLE:
        st.warning("Storage set to 'sheets', but gspread isn't installed. Falling back to CSV.")
    else:
        st.info("Storage: CSV (fallback)")

    role = st.segmented_control("Login as", options=["Team", "Admin"], default="Team")

    if role == "Team":
        teams = get_df("teams")
        names = ["(Select)"] + teams["name"].tolist()
        name = st.selectbox("Team", names)
        pin = st.text_input("PIN", type="password")
        if st.button("Enter"):
            if name == "(Select)":
                st.error("Choose your team.")
            else:
                tid = auth_team(name, pin)
                if tid:
                    st.session_state["auth"] = {"role": "team", "team_id": tid}
                    st.experimental_rerun()
                else:
                    st.error("Invalid PIN.")
    else:
        apin = st.text_input("Admin PIN", type="password")
        if st.button("Enter as admin"):
            if auth_admin(apin):
                st.session_state["auth"] = {"role": "admin"}
                st.experimental_rerun()
            else:
                st.error("Invalid admin PIN.")

    st.divider()
    st.markdown("#### First run / Setup")
    st.caption("If there are no teams yet, create a few in Admin > Teams after logging in with default PIN 3141.")


# -------------------- MAIN --------------------

def main():
    st.set_page_config(page_title="EcoBank — powering your growth", page_icon="💸", layout="wide")
    init_files()

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
        admin_console()


if __name__ == "__main__":
    main()
