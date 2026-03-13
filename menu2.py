from __future__ import annotations

import datetime
import json
import re
from html import escape
from zoneinfo import ZoneInfo

from pathlib import Path  #추가됨

import pandas as pd
import streamlit as st
from streamlit.components.v1 import html

from fnc2 import (
    fetch_mgmt,
    fetch_caution,
    fetch_delist,
    fetch_lp,
)

#추가됨
TARGET_CODE_FILE = "선정종목 100개.xlsx"

#추가됨
def find_target_excel_file(file_name: str) -> str | None:
    """
    실행 위치 기준으로 엑셀 파일명을 자동 탐색
    우선순위:
    1) 현재 작업 폴더
    2) 현재 스크립트 파일 폴더
    3) 하위 폴더 전체 재귀 탐색
    """
    cwd_path = Path.cwd() / file_name
    if cwd_path.exists():
        return str(cwd_path)

    try:
        base_dir = Path(__file__).resolve().parent
    except NameError:
        base_dir = Path.cwd()

    script_path = base_dir / file_name
    if script_path.exists():
        return str(script_path)

    matches = list(base_dir.rglob(file_name))
    if matches:
        return str(matches[0])

    return None


#추가됨
def load_target_codes_from_excel(
    file_name: str,
    sheet_name=0,
    code_col_idx: int = 1,   # B열 = index 1
    skip_rows: int = 0,
) -> set[str]:
    """
    엑셀 파일명을 받아 자동으로 파일 위치를 찾고,
    해당 파일의 B열 종목코드를 읽어 set으로 반환
    """
    found_path = find_target_excel_file(file_name)
    if not found_path:
        return set()

    df_codes = pd.read_excel(
        found_path,
        sheet_name=sheet_name,
        usecols=[code_col_idx],
        dtype=str,
        skiprows=skip_rows,
        header=None,
    )

    codes = (
        df_codes.iloc[:, 0]
        .dropna()
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .str.zfill(6)
    )

    codes = codes[codes.str.match(r"^[0-9A-Z]{6}$", na=False)]

    return set(codes.tolist())


#추가됨
def find_code_column(df: pd.DataFrame) -> str | None:
    """
    원본 공시 데이터에서 종목코드 컬럼명을 탐색
    """
    candidates = [
        "종목코드", "단축코드", "code", "Code", "CODE",
        "isuCd", "ISU_CD", "표준코드"
    ]
    for c in candidates:
        if c in df.columns:
            return c
    return None
    
# ─────────────────────────────────────────────────────────────
# 메뉴 스펙
# ─────────────────────────────────────────────────────────────
MENU_SPEC = [
    ("multi",   "✅ KRX 전체 모아보기", 0),
    ("mgmt",    "1️⃣ 관리종목", 1),
    ("caution", "2️⃣ 투자유의", 1),
    ("delist",  "3️⃣ 상장폐지", 1),
    ("lp",      "4️⃣ 유동성공급(LP)", 1),
]

# 실제 동작 맵
FETCHER_MAP = {
    "multi": "multi",
    "mgmt": "mgmt",
    "caution": "caution",
    "delist": "delist",
    "lp": "lp",
}

# 라벨 포맷터(들여쓰기: U+2003 EM SPACE)
def _menu_label(key: str) -> str:
    for k, label, level in MENU_SPEC:
        if k == key:
            return (" " * level) + label
    return key


# 주말이면 가장 가까운 이전 평일로
def _last_weekday(d: datetime.date) -> datetime.date:
    wd = d.weekday()  # 월0..일6
    if wd == 5:  # 토
        return d - datetime.timedelta(days=1)
    if wd == 6:  # 일
        return d - datetime.timedelta(days=2)
    return d


# 날짜 안전 보정
def _coerce_date_pair(s, e, default_start, default_end):
    import datetime as _dt
    if not isinstance(s, _dt.date):
        s = default_start
    if not isinstance(e, _dt.date):
        e = default_end
    if s > e:
        s, e = e, s
    return s, e


# 당일 하이라이트
def style_today_rows(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    highlight = "background-color: #e8f4ff; font-weight: 600;"
    def _row_style(row: pd.Series):
        return [highlight] * len(row) if row.get("당일", "") == "🟡" else [""] * len(row)
    return df.style.apply(_row_style, axis=1)


# 화면 표시용 변환 (시간 포맷: yy/mm/dd HH:MM)
def build_display_df(df: pd.DataFrame, ref_date: datetime.date) -> pd.DataFrame:
    ts = pd.to_datetime(df.get("시간", ""), errors="coerce")
    time_disp = ts.dt.strftime("%y/%m/%d %H:%M").fillna("")
    is_today = ts.dt.date.eq(ref_date)

    out = (
        pd.DataFrame({
            "당일": is_today.map(lambda x: "🟡" if x else ""),
            "시간": time_disp,
            "종목명": df.get("회사명", "").astype(str),
            "공시제목": df.get("뷰어URL", "").astype(str),
        })
        .sort_values("시간", ascending=False)
        .reset_index(drop=True)
    )
    return out


# 공시제목/링크 분리 복사용
def _split_title_and_link(url_series: pd.Series) -> tuple[pd.Series, pd.Series]:
    url_series = url_series.astype(str)
    title = url_series.str.extract(r"#(.+)$")[0]
    title = title.where(title.notna() & (title != ""), url_series)
    link = url_series.str.replace(r"#.+$", "", regex=True)
    return title, link


def _make_copy_df(df_display: pd.DataFrame) -> pd.DataFrame:
    tmp = df_display.copy()

    title, link = _split_title_and_link(tmp["공시제목"])
    tmp["공시제목"], tmp["링크"] = title, link

    cols = ["당일", "시간", "종목명", "공시제목", "링크"]
    return tmp[cols]


# 행 수 기반 dataframe 높이 자동 조절
def _df_height(
    df: pd.DataFrame,
    base_row_height: int = 30,
    header_height: int = 35,
    max_height: int = 550,
    min_height: int = 150,
) -> int:
    rows = max(len(df), 1)
    h = header_height + base_row_height * rows + 15
    if h < min_height:
        h = min_height
    if h > max_height:
        h = max_height
    return h


def render_header_with_copy(copy_id: str, caption_text: str, df_display: pd.DataFrame):
    """
    캡션(좌) + 복사 버튼(우)을 한 줄에 배치
    """
    safe_caption = escape(caption_text).replace("\n", "<br>")

    copy_df = _make_copy_df(df_display)
    clipboard = copy_df.to_csv(sep="\t", index=False)
    js_text = json.dumps(clipboard)

    col1, col2 = st.columns([5, 1.5])
    with col1:
        st.markdown(
            f"""
            <div style="
                display:flex;
                align-items:flex-end;
                height:100%;
                margin: 0 0 2px 0;
                font-size: 0.9rem;
                line-height: 1.2;
                color: rgba(49,51,63,0.75);
            ">{safe_caption}</div>
            """,
            unsafe_allow_html=True,
        )
    with col2:
        html(
            f"""
            <div style="
                display:flex;
                justify-content:flex-end;
                align-items:flex-end;
                margin: 0 0 2px 0;
            ">
              <button id="{copy_id}" onclick="copy_{copy_id}()" style="
                  font-size:15px; padding:6px 12px; width:180px;
                  background-color:#4CAF50; color:white; border:none; border-radius:4px;">
                  📋 복사
              </button>
            </div>
            <script>
            function copy_{copy_id}() {{
                const text = {js_text};
                navigator.clipboard.writeText(text).then(() => {{
                    var b=document.getElementById("{copy_id}");
                    b.innerText="✅ 복사 완료"; b.style.backgroundColor="#777";
                    setTimeout(()=>{{b.innerText="📋 복사"; b.style.backgroundColor="#4CAF50";}},2000);
                }});
            }}
            </script>
            """,
            height=50,
        )


# ─────────────────────────────────────────────────────────────
# 데이터 페치
# ─────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=60)
def _fetch(menu_key: str, f: str, t: str, page_size: int = 100, nonce: int = 0) -> pd.DataFrame:
    _ = nonce
    ftype = FETCHER_MAP[menu_key]

    if ftype == "mgmt":
        return fetch_mgmt(f, t, page_size=page_size)

    if ftype == "caution":
        return fetch_caution(f, t, page_size=page_size)

    if ftype == "delist":
        return fetch_delist(f, t, page_size=page_size)

    if ftype == "lp":
        return fetch_lp(f, t, page_size=page_size)

    if ftype == "multi":
        return _fetch_multi(f, t, page_size=page_size, nonce=nonce)

    return pd.DataFrame()


@st.cache_data(show_spinner=False, ttl=60)
def _fetch_multi(f: str, t: str, page_size: int = 100, nonce: int = 0) -> pd.DataFrame:
    _ = nonce

    df_mgmt = fetch_mgmt(f, t, page_size=page_size)
    df_caution = fetch_caution(f, t, page_size=page_size)
    df_delist = fetch_delist(f, t, page_size=page_size)
    df_lp = fetch_lp(f, t, page_size=page_size)

    dfs = [x for x in [df_mgmt, df_caution, df_delist, df_lp] if x is not None and not x.empty]
    if not dfs:
        return pd.DataFrame()

    merged = pd.concat(dfs, ignore_index=True, sort=False)

    if "문서번호" in merged.columns:
        merged = merged.drop_duplicates(subset=["문서번호"], keep="first")

    if "시간" in merged.columns:
        merged["__ts"] = pd.to_datetime(merged["시간"], errors="coerce")
        merged = merged.sort_values("__ts", ascending=False).drop(columns="__ts")

    return merged.reset_index(drop=True)


# ─────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────
def run():
    st.set_page_config(
        page_title="KRX ETF 공시 모니터",
        layout="centered",
        initial_sidebar_state="expanded",
    )

    # 사이드바 너비 + 라디오 간격 CSS
    SIDEBAR_PX = 380
    st.markdown(f"""
    <style>
    [data-testid="stSidebar"] {{
      min-width: {SIDEBAR_PX}px; max-width: {SIDEBAR_PX}px;
    }}
    [data-testid="stSidebar"] > div:first-child {{ width: {SIDEBAR_PX}px; }}
    #menu-radio-wrap [role="radiogroup"] {{
      display: flex; flex-direction: column; row-gap: 10px;
    }}
    #menu-radio-wrap [role="radiogroup"] > *:hover {{
      background: rgba(0,0,0,0.03); border-radius: 8px;
    }}
    @media (max-width: 1100px) {{
      [data-testid="stSidebar"] {{ min-width: 320px; max-width: 320px; }}
      [data-testid="stSidebar"] > div:first-child {{ width: 320px; }}
    }}
    </style>
    """, unsafe_allow_html=True)

    st.markdown("### 📡 KRX ETF 공시 모니터")

    if "menu_cache" not in st.session_state:
        st.session_state["menu_cache"] = {}
    if "force_nonce" not in st.session_state:
        st.session_state["force_nonce"] = 0

    # ── 사이드바
    with st.sidebar:
        # 1) 기간
        st.markdown("## 📆 KIND 조회 기간")
        today_kst = datetime.datetime.now(ZoneInfo("Asia/Seoul")).date()
        five_days_ago = today_kst - datetime.timedelta(days=5)

        c1, c2 = st.columns(2)
        with c1:
            start_date = st.date_input("시작일", value=five_days_ago, format="YYYY/MM/DD", key="start_date")
        with c2:
            end_date = st.date_input("종료일", value=today_kst, format="YYYY/MM/DD", key="end_date")

        d_start, d_end = _coerce_date_pair(start_date, end_date, five_days_ago, today_kst)

        st.markdown("---")

        # 2) 메뉴
        st.markdown("## ⚠️ KIND 공시 조회")
        st.markdown('<div id="menu-radio-wrap">', unsafe_allow_html=True)
        menu_key = st.radio(
            "카테고리 선택",
            options=[k for k, _, _ in MENU_SPEC],
            index=0,
            format_func=_menu_label,
            label_visibility="collapsed",
        )
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("---")

        # 3) 검색
        st.markdown("## 🔎 제목/종목/시간 검색")
        keyword = st.text_input(
            "공시제목 / 종목명 포함",
            value="",
            label_visibility="collapsed",
            placeholder="*(공란가능)키워드 입력",
        )
        case_sens = False

        only_target = st.checkbox("엑셀 B열 종목코드만 보기", value=False)  #추가됨

        # 4) 조회 시간
        TIME_START = [("00:00", datetime.time(0, 0)),
                      ("14:30", datetime.time(14, 30))]
        TIME_END   = [("09:00", datetime.time(9, 0)),
                      ("23:59", datetime.time(23, 59))]

        start_labels = [lbl for lbl, _ in TIME_START]
        end_labels = [lbl for lbl, _ in TIME_END]
        map_start = {lbl: tm for lbl, tm in TIME_START}
        map_end = {lbl: tm for lbl, tm in TIME_END}

        cst, cet = st.columns(2)
        with cst:
            start_time_lbl = st.selectbox(
                "시작",
                options=start_labels,
                index=0,
                key="start_time_lbl",
                label_visibility="collapsed",
            )
        with cet:
            end_time_lbl = st.selectbox(
                "종료",
                options=end_labels,
                index=len(end_labels) - 1,
                key="end_time_lbl",
                label_visibility="collapsed",
            )

        # 5) 조회/캐시 제어 버튼들
        go = st.button("공시 조회", type="primary", use_container_width=True)

        cA, cB = st.columns(2)
        with cA:
            if st.button("🔄 강제 새로조회", use_container_width=True):
                st.session_state["force_nonce"] += 1
                st.toast("캐시 무시하고 다시 조회합니다.", icon="🔄")
        with cB:
            if st.button("🧹 초기화", use_container_width=True):
                st.cache_data.clear()
                st.cache_resource.clear()
                st.session_state.clear()
                st.toast("캐시/세션을 초기화했습니다.", icon="🧹")
                st.rerun()

    if d_start > d_end:
        st.error("시작일이 종료일보다 이후입니다.")
        return

    f = d_start.strftime("%Y-%m-%d")
    t = d_end.strftime("%Y-%m-%d")

    # 수집/캐시
    cache_key = (menu_key, f, t)
    df_raw: pd.DataFrame | None = None

    if go:
        try:
            with st.spinner(f"KIND에서 [{_menu_label(menu_key).strip()}] 데이터 수집 중..."):
                df_raw = _fetch(menu_key, f, t, page_size=100, nonce=st.session_state["force_nonce"])
        except Exception as e:
            st.error("KIND 응답이 비정상입니다(차단/오류 가능).")
            st.code(str(e))
            st.info("🔄 강제 새로조회 → 안 되면 🧹 초기화 → 그래도 안 되면 조회기간을 줄이거나 fnc2.py의 sleep을 늘려보세요.")
            return

        if df_raw.empty:
            st.warning("해당 조건에 일치하는 데이터가 없습니다.")
            return

        ts_kst = datetime.datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S KST")
        st.session_state["menu_cache"][cache_key] = {"time_kst": ts_kst, "raw": df_raw}
    else:
        bundle = st.session_state["menu_cache"].get(cache_key)
        if bundle:
            df_raw = bundle.get("raw")

    # 수집 전
    if df_raw is None:
        st.info("기간과 카테고리 선택 후 **공시 조회**를 먼저 눌러주세요. (검색/조회 시간은 이후엔 즉시 필터만 적용)")
        return

    if df_raw.empty:
        st.warning("해당 조건에 일치하는 데이터가 없습니다.")
        return

    # (0) 엑셀 종목코드 필터 #추가됨
    df_view = df_raw.copy()

    if only_target:
        target_codes = load_target_codes_from_excel(TARGET_CODE_FILE_NAME)

        if not target_codes:
            st.warning(f"엑셀 파일을 찾지 못했거나 종목코드를 읽지 못했습니다: {TARGET_CODE_FILE_NAME}")
            return

        code_col = find_code_column(df_view)

        if code_col is None:
            st.warning("원본 공시 데이터에 종목코드 컬럼이 없습니다. fnc2.py에서 종목코드도 함께 수집해야 합니다.")
            st.write("현재 컬럼 목록:", df_view.columns.tolist())  #추가됨
            return

        df_view[code_col] = (
            df_view[code_col]
            .astype(str)
            .str.strip()
            .str.replace(r"\.0$", "", regex=True)
            .str.zfill(6)
        )

        df_view = df_view[df_view[code_col].isin(target_codes)]

        if df_view.empty:
            st.warning("엑셀 B열 종목코드 기준으로 필터링한 결과가 없습니다.")
            return
            
    # (1) 키워드 필터
    df_view = df_raw.copy()
    if keyword.strip():
        flags = 0 if case_sens else re.IGNORECASE
        patt = re.compile(re.escape(keyword.strip()), flags)

        mask = (
            df_view.get("공시제목", "").astype(str).str.contains(patt, na=False) |
            df_view.get("회사명", "").astype(str).str.contains(patt, na=False)
        )
        df_view = df_view[mask]

    # (2) 조회 시간 필터
    st_tm = map_start[start_time_lbl]
    en_tm = map_end[end_time_lbl]
    if not df_view.empty:
        ts_all = pd.to_datetime(df_view["시간"], errors="coerce")
        tt = ts_all.dt.time
        if st_tm <= en_tm:
            mask_time = (tt >= st_tm) & (tt <= en_tm)
        else:
            mask_time = (tt >= st_tm) | (tt <= en_tm)
        df_view = df_view[mask_time]

    if df_view.empty:
        st.warning("필터 조건에 해당하는 데이터가 없습니다.")
        return

    # 표시용 변환
    ref_date = _last_weekday(d_end)
    df_all_show = build_display_df(df_view, ref_date)

    caption_head = f"\n선택: {_menu_label(menu_key).strip()} · 기간: {f} ~ {t} · 총 {len(df_all_show)}건"

    colcfg = {
        "당일": st.column_config.TextColumn(width=35),
        "시간": st.column_config.TextColumn(width=98),
        "종목명": st.column_config.TextColumn(width=120),
        "공시제목": st.column_config.LinkColumn(
            "공시제목", width=360, help="클릭하면 KRX 뷰어로 이동합니다", display_text=r"#(.+)$"
        ),
    }

    # 탭 1개 유지
    tab1, = st.tabs(["1) KRX 전체 종목"])

    with tab1:
        render_header_with_copy("copy_tab1", caption_head, df_all_show)
        styled = style_today_rows(df_all_show)
        st.dataframe(
            styled,
            use_container_width=True,
            hide_index=True,
            height=_df_height(df_all_show),
            column_config=colcfg,
        )


if __name__ == "__main__":
    run()
