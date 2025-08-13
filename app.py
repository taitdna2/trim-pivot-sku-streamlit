# app_trim_pivot.py
import os
from io import BytesIO
from collections import Counter
from typing import List, Optional

import pandas as pd
import streamlit as st

# =========================
# CẤU HÌNH CHUNG
# =========================
st.set_page_config(page_title="CẮT & PIVOT SKU", layout="wide")
st.title("✂️ CẮT FILE & 📊 PIVOT SẢN LƯỢNG/DOANH SỐ THEO KH")
st.caption("Bước 1: cắt cột bằng pandas (giữ nguyên số dòng).  Bước 2: pivot theo khách hàng, có lọc & tải Excel.")

# Cột theo vị trí (0-based): D(3), L(11), M(12), Q(16), R(17), S(18), W(22), Z(25)
COL_INDICES = [3, 11, 12, 16, 17, 18, 22, 25]

# Cột bắt buộc cho PIVOT
REQUIRED = [
    "Tên NPP", "Mã KH", "Tên KH", "Nhóm hàng",
    "Mã SP", "Tên SP", "Tổng Sản lượng (Lẻ)", "Doanh số bán"
]

# Nếu header của file “đã cắt” chưa đúng tên → map nhanh theo vị trí
INDEX_TO_REQUIRED = {
    0: "Tên NPP",               # D
    1: "Mã KH",                 # L
    2: "Tên KH",                # M
    3: "Nhóm hàng",             # Q
    4: "Mã SP",                 # R
    5: "Tên SP",                # S
    6: "Tổng Sản lượng (Lẻ)",   # W
    7: "Doanh số bán",          # Z
}


# =========================
# HELPERS
# =========================
def _mode_text(series):
    vals = [str(x).strip() for x in series if str(x).strip() and str(x).strip().lower() != "nan"]
    if not vals:
        return ""
    cnt = Counter(vals)
    mx = max(cnt.values())
    return sorted([v for v, c in cnt.items() if c == mx])[0]


def cut_with_pandas_keep_rows(file_bytes: bytes, sheet_name: Optional[str]) -> bytes:
    """
    CẮT file bằng pandas (đọc full sheet -> đảm bảo GIỮ nguyên số dòng),
    chỉ lấy các cột theo COL_INDICES. Trả về bytes (.xlsx).
    """
    # Xác định sheet
    xls = pd.ExcelFile(BytesIO(file_bytes), engine="openpyxl")
    real_sheet = sheet_name if (sheet_name and sheet_name in xls.sheet_names) else xls.sheet_names[0]

    # Đọc full sheet (giữ tất cả dòng)
    df_all = pd.read_excel(xls, sheet_name=real_sheet, dtype=object, keep_default_na=False, engine="openpyxl")

    # Kiểm tra đủ số cột để cắt
    max_idx = max(COL_INDICES)
    if df_all.shape[1] <= max_idx:
        raise ValueError(f"Sheet '{real_sheet}' chỉ có {df_all.shape[1]} cột, cần tới index {max_idx}. Kiểm tra lại.")

    # Cắt theo index
    df_cut = df_all.iloc[:, COL_INDICES]

    # Xuất Excel ra bytes
    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        df_cut.to_excel(w, index=False, sheet_name="Trimmed")
    return out.getvalue()


def normalize_after_cut(df: pd.DataFrame, header_row_user: int) -> pd.DataFrame:
    """
    Chuẩn hoá tên cột sau khi cắt (file đã có 8 cột).
    - header_row_user: dòng tiêu đề (1-based) do người dùng chọn.
    - Nếu tên chưa ổn → map theo INDEX_TO_REQUIRED.
    """
    # Nếu người dùng chỉ định header khác dòng 1 → đọc lại dùng header đó
    if header_row_user != 1:
        buf = BytesIO()
        df.to_excel(buf, index=False, header=False)
        buf.seek(0)
        df = pd.read_excel(buf, header=header_row_user - 1, engine="openpyxl")

    # Chuẩn tên về string
    df.columns = [str(c).strip() for c in df.columns]

    # Map theo vị trí nếu cần
    ren = {}
    for i, c in enumerate(df.columns[:len(INDEX_TO_REQUIRED)]):
        target = INDEX_TO_REQUIRED.get(i)
        if target:
            ren[c] = target
    df = df.rename(columns=ren)

    # Đảm bảo đủ các cột REQUIRED (thiếu thì tạo rỗng)
    for c in REQUIRED:
        if c not in df.columns:
            df[c] = None
    df = df[REQUIRED].copy()

    # Chuẩn kiểu số
    df["Tổng Sản lượng (Lẻ)"] = pd.to_numeric(df["Tổng Sản lượng (Lẻ)"], errors="coerce").fillna(0)
    df["Doanh số bán"] = pd.to_numeric(df["Doanh số bán"], errors="coerce").fillna(0)

    # Chuẩn chuỗi
    for c in ["Tên NPP", "Mã KH", "Tên KH", "Nhóm hàng", "Mã SP", "Tên SP"]:
        df[c] = df[c].astype(str).str.strip()

    return df


def build_pivot_by_customer(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot: mỗi hàng = 1 Mã KH
    - Cột động theo Tên SP (tổng sản lượng)
    - Cột 'Tổng Doanh số'
    - 2 cột đại diện: 'Tên KH đại diện', 'Tên NPP đại diện' (mode)
    """
    rep = (
        df.groupby("Mã KH")
          .agg(**{
              "Tên KH đại diện": ("Tên KH", _mode_text),
              "Tên NPP đại diện": ("Tên NPP", _mode_text),
          })
          .reset_index()
    )

    qty = pd.pivot_table(
        df,
        index=["Mã KH"],
        columns="Tên SP",
        values="Tổng Sản lượng (Lẻ)",
        aggfunc="sum",
        fill_value=0,
        observed=False,
    )
    revenue = df.groupby("Mã KH", as_index=True)["Doanh số bán"].sum().to_frame("Tổng Doanh số")

    out = rep.set_index("Mã KH").join(qty, how="right").join(revenue, how="left").reset_index()

    fixed = ["Mã KH", "Tên KH đại diện", "Tên NPP đại diện"]
    cols = out.columns.tolist()
    dynamic = [c for c in cols if c not in fixed and c != "Tổng Doanh số"]
    if "Tổng Doanh số" in cols:
        dynamic.append("Tổng Doanh số")
    return out[fixed + dynamic]


# =========================
# UI — Bước 1: CẮT FILE
# =========================
st.header("Bước 1 — ✂️ Cắt cột từ file nặng (pandas, giữ nguyên số dòng)")
with st.expander("Chọn & cắt file", expanded=True):
    c1, c2 = st.columns([2, 1])
    with c1:
        raw_file = st.file_uploader(
            "Upload file Excel GỐC (xlsx/xlsm/xls; nếu xlsb hãy lưu lại .xlsx trước khi dùng)",
            type=["xlsx", "xlsm", "xls"],
            key="raw_file",
        )
    with c2:
        sheet_name = None
        if raw_file:
            try:
                xls = pd.ExcelFile(raw_file, engine="openpyxl")
                sheet_name = st.selectbox(
                    "Chọn sheet cần cắt",
                    xls.sheet_names,
                    index=0,
                    key="cut_sheet",
                )
            except Exception as e:
                st.warning(f"Không đọc được danh sách sheet (sẽ dùng sheet đầu tiên). Chi tiết: {e}")
                sheet_name = None

    # Nút cắt
    if raw_file and st.button("✂️ CẮT NGAY", key="btn_cut", use_container_width=True):
        try:
            with st.spinner("Đang cắt cột bằng pandas..."):
                trimmed_bytes = cut_with_pandas_keep_rows(
                    file_bytes=raw_file.getvalue(),
                    sheet_name=sheet_name,
                )
            st.session_state["trimmed_bytes"] = trimmed_bytes
            st.success("✅ Đã cắt xong. Tải về hoặc dùng trực tiếp cho Bước 2.")
            st.download_button(
                "⬇️ Tải file đã cắt (.xlsx)",
                data=trimmed_bytes,
                file_name=f"{os.path.splitext(raw_file.name)[0]}_filtered_preserve.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        except Exception as e:
            st.error(f"Lỗi khi cắt file: {e}")


# =========================
# UI — Bước 2: PIVOT
# =========================
st.header("Bước 2 — 📊 Pivot sản lượng & doanh số theo Khách Hàng")

source_choice = st.radio(
    "Chọn nguồn file ‘đã cắt’ để pivot:",
    ["Dùng file đã cắt ở Bước 1", "Upload file đã cắt (8 cột)"],
    horizontal=True,
    index=0,
)

trimmed_to_use = None
if source_choice == "Dùng file đã cắt ở Bước 1":
    if "trimmed_bytes" in st.session_state:
        trimmed_to_use = st.session_state["trimmed_bytes"]
    else:
        st.info("Chưa có file đã cắt trong session. Hãy thực hiện Bước 1 hoặc chọn 'Upload file đã cắt'.")
else:
    up2 = st.file_uploader("Upload file ĐÃ CẮT (8 cột)", type=["xlsx"], key="trimmed_upload")
    if up2:
        trimmed_to_use = up2.getvalue()

header_row_user = st.number_input(
    "Dòng tiêu đề trong file đã cắt (1 = dòng đầu)",
    min_value=1, value=1, step=1, key="pivot_header_row"
)

if trimmed_to_use and st.button("🚀 PIVOT NGAY", use_container_width=True, key="btn_pivot"):
    try:
        with st.spinner("Đang đọc & chuẩn hoá..."):
            # đọc sheet đầu tiên (Trimmed)
            df_cut = pd.read_excel(BytesIO(trimmed_to_use), engine="openpyxl", header=0)
            df_norm = normalize_after_cut(df_cut, header_row_user=header_row_user)

            miss = [c for c in REQUIRED if c not in df_norm.columns]
            if miss:
                st.error(f"Thiếu cột bắt buộc: {miss}")
                st.stop()

        with st.spinner("Đang pivot theo Khách Hàng..."):
            pivot_df = build_pivot_by_customer(df_norm)

        st.success("✅ Hoàn tất! Bảng pivot ở dưới:")
        st.dataframe(pivot_df, use_container_width=True)

        # Tải Excel pivot
        out = BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as w:
            pivot_df.to_excel(w, index=False, sheet_name="PIVOT_KH")
        st.download_button(
            "⬇️ Tải Excel PIVOT",
            data=out.getvalue(),
            file_name="pivot_sku_theo_khachhang.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

        # Lọc nhanh
        with st.expander("🔎 Lọc nhanh"):
            col1, col2 = st.columns(2)
            with col1:
                kw = st.text_input("Tìm theo Mã KH / Tên KH / Tên NPP", "")
            with col2:
                min_rev = st.number_input("Lọc Tổng Doanh số ≥", min_value=0, value=0, step=50_000)

            filt = pivot_df.copy()
            if kw:
                k = kw.lower().strip()
                search_cols = [c for c in filt.columns if c in ["Mã KH", "Tên KH đại diện", "Tên NPP đại diện"]]
                mask = False
                for c in search_cols:
                    ser = filt[c].astype(str).str.lower().str.contains(k, na=False)
                    mask = ser if isinstance(mask, bool) else (mask | ser)
                if not isinstance(mask, bool):
                    filt = filt[mask]
            if "Tổng Doanh số" in filt.columns:
                filt = filt[pd.to_numeric(filt["Tổng Doanh số"], errors="coerce").fillna(0) >= min_rev]

            st.dataframe(filt, use_container_width=True)

    except Exception as e:
        st.error(f"Lỗi khi pivot: {e}")
