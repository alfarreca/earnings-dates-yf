import io
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
import yfinance as yf # <-- MOVED yfinance import to the top

# ------------------------------
# App Config
# ------------------------------
st.set_page_config(page_title="Earnings Date Fetcher (yfinance)", layout="wide")
st.title("📅 Earnings Date Fetcher — yfinance")
st.caption("Upload an Excel file with a **Symbol** column. I'll fetch the next earnings date for each using yfinance.")

# Debug panel (helps diagnose Streamlit Cloud issues)
with st.sidebar:
    st.subheader("⚙️ Runtime Info")
    st.write({
        "python": sys.version,
        "time": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
    })
    try:
        # Check the version of the already-imported yf
        st.write({"yfinance_version": getattr(yf, "__version__", "unknown")})
    except Exception as e:
        # This error is now less likely to cause a hang, as the import is global
        st.warning(f"yfinance version check error: {e}")

# Note: Removed the duplicate st.title and st.caption here.

# ------------------------------
# Helpers
# ------------------------------
@st.cache_data(ttl=60 * 30, show_spinner=False)
def fetch_from_yf(symbol: str) -> dict:
    """Fetch next earnings date using yfinance, with multiple fallbacks.
    Returns a dict with fields: Symbol, NextEarningsDate, Source, Details, Error.
    """
    # yfinance is now globally imported, no need to import inside the function.

    result = {
        "Symbol": symbol,
        "NextEarningsDate": None,
        "Source": None,
        "Details": None,
        "Error": None,
    }

    try:
        t = yf.Ticker(symbol)
        # Try the modern endpoint first
        dt_candidates = []
        used = None

        # 1) get_earnings_dates (preferred in recent yfinance versions)
        try:
            df = t.get_earnings_dates(limit=12)
            if df is not None and isinstance(df, pd.DataFrame) and len(df) > 0:
                # Some yfinance builds return DatetimeIndex; others put date in a column
                if isinstance(df.index, pd.DatetimeIndex):
                    dates = df.index.to_pydatetime().tolist()
                else:
                    # Look for likely date columns
                    date_col = None
                    for c in df.columns:
                        if "date" in c.lower():
                            date_col = c
                            break
                    if date_col is not None:
                        dates = pd.to_datetime(df[date_col], errors="coerce").dropna().to_list()
                    else:
                        dates = []
                dt_candidates.extend(dates)
                used = "get_earnings_dates"
        except Exception as e:
            # Fall through to next method
            pass

        # 2) legacy calendar (older yfinance). Often returns a 1-row DF with column 'Earnings Date'
        if not dt_candidates:
            try:
                cal = t.calendar
                if cal is not None and isinstance(cal, pd.DataFrame) and not cal.empty:
                    # Common patterns: index has rows like 'Earnings Date'; values can be pd.Timestamp or object list
                    if "Earnings Date" in cal.index:
                        val = cal.loc["Earnings Date"].values[0]
                        if isinstance(val, (pd.Timestamp, datetime)):
                            dt_candidates = [pd.to_datetime(val)]
                        else:
                            # Sometimes it's a list/array like [start, end] — take the first non-null
                            try:
                                seq = list(val) if hasattr(val, "__iter__") else [val]
                                seq = [pd.to_datetime(x, errors="coerce") for x in seq]
                                seq = [x for x in seq if pd.notna(x)]
                                dt_candidates = seq
                            except Exception:
                                pass
                        used = "calendar:index"
                    elif "Earnings Date" in cal.columns:
                        # Occasionally appears as a column
                        dates = pd.to_datetime(cal["Earnings Date"], errors="coerce").dropna().tolist()
                        dt_candidates = dates
                        used = "calendar:column"
            except Exception:
                pass

        # Normalize & choose the *next* earnings date (>= today UTC). If none in future, choose the most recent.
        now = datetime.now(timezone.utc)
        clean = []
        for d in dt_candidates:
            try:
                ts = pd.to_datetime(d, utc=True).to_pydatetime()
                clean.append(ts)
            except Exception:
                continue
        clean = sorted(set(clean))

        next_dt = None
        if clean:
            future = [d for d in clean if d >= now]
            next_dt = future[0] if future else clean[-1]

        if next_dt is not None:
            result["NextEarningsDate"] = next_dt.isoformat()
            result["Source"] = used or "yfinance"
            result["Details"] = f"candidates={len(clean)}"
        else:
            result["Error"] = "No date found via yfinance endpoints"

    except Exception as e:
        result["Error"] = str(e)

    return result


def to_excel_download(df: pd.DataFrame) -> bytes:
    with io.BytesIO() as buffer:
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="EarningsDates", index=False)
        return buffer.getvalue()

# ------------------------------
# UI — File input & options
# ------------------------------
uploaded = st.file_uploader("Upload Excel (.xlsx) with a 'Symbol' column", type=["xlsx"])
max_workers = st.slider("Concurrency (workers)", min_value=2, max_value=16, value=8, help="Parallel requests to yfinance")

run = st.button("Fetch Earnings Dates", type="primary", disabled=uploaded is None)

if run and uploaded is not None:
    try:
        raw = pd.read_excel(uploaded)
    except Exception as e:
        st.error(f"Failed to read Excel: {e}")
        st.stop()

    if "Symbol" not in raw.columns:
        st.error("The uploaded file must contain a 'Symbol' column.")
        st.stop()

    # Preprocess symbols
    symbols = (
        raw["Symbol"].astype(str).str.strip().str.upper().replace({"": pd.NA}).dropna().unique().tolist()
    )

    if not symbols:
        st.warning("No symbols found after cleaning.")
        st.stop()

    st.info(f"Processing {len(symbols)} symbols…")
    progress = st.progress(0)
    status = st.empty()

    out_rows = []
    completed = 0

    # Threaded fetching
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        fut_map = {ex.submit(fetch_from_yf, s): s for s in symbols}
        for fut in as_completed(fut_map):
            res = fut.result()
            out_rows.append(res)
            completed += 1
            progress.progress(int(completed / len(symbols) * 100))
            status.write(f"Fetched: {res['Symbol']} — " + (res.get("NextEarningsDate") or res.get("Error") or ""))

    out = pd.DataFrame(out_rows)

    # Merge back to original order, preserving original columns and adding the results
    merged = raw.copy()
    merged["_SYMBOL_UPPER_"] = merged["Symbol"].astype(str).str.upper()
    out_map = out.set_index("Symbol")[
        ["NextEarningsDate", "Source", "Details", "Error"]
    ]
    merged = merged.join(out_map, on="_SYMBOL_UPPER_")
    merged.drop(columns=["_SYMBOL_UPPER_"], inplace=True)

    st.subheader("Results")
    st.dataframe(merged, use_container_width=True)

    xls_bytes = to_excel_download(merged)
    st.download_button(
        label="📥 Download results (Excel)",
        data=xls_bytes,
        file_name="earnings_dates_yfinance.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.caption(
        "Note: yfinance may return *estimated* dates or none for some international tickers. "
        "When a date cannot be found, 'Error' will indicate why. For trading around events, verify on the company's IR page."
    )

else:
    st.info("Upload your workbook and click **Fetch Earnings Dates** to begin.")
