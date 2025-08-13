# app_trim_pivot.py
import io
import os
import pandas as pd
import streamlit as st
from collections import Counter
from io import BytesIO
from typing import List, Optional

st.set_page_config(page_title="CẮT & PIVOT SKU", layout="wide")
st.title("✂️ CẮT FILE & 📊 PIVOT SẢN LƯỢNG/DOANH SỐ THEO KH")
st.caption("Tối ưu cho file Excel lớn: Bước 1 cắt cột bằng streaming (không load toàn bộ), Bước 2 pivot theo khách hàng.")

# =========================
# CẤU HÌNH BƯỚC 1 (CẮT CỘT)
# =========================
# Cột theo vị trí (0-based): D(3), L(11), M(12), Q(16), R(17), S(18), W(22), Z(25)
COL_INDICES = [3, 11, 12, 16, 17, 18, 22, 25]

# =========================
# CẤU HÌNH BƯỚC 2 (PIVOT)
# =========================
REQUIRED = [
    "Tên NPP", "Mã KH", "Tên KH", "Nhóm hàng",
    "Mã SP", "Tên SP", "Tổng Sản lượng (Lẻ)", "Doanh số bán"
]

# Nếu hàng tiêu đề của file “đã cắt” không đúng tên, ta map nhanh theo chỉ số cột
INDEX_TO_REQUIRED = {
    0: "Tên NPP",                 # tương ứng D ban đầu (ví dụ)
    1: "Mã KH",                   # L
    2: "Tên KH",                  # M
    3: "Nhóm hàng",               # Q
    4: "Mã SP",                   # R
    5: "Tên SP",                  # S
    6: "Tổng Sản lượng (Lẻ)",    # W
    7: "Doanh số bán",           # Z
}

# ========= Helpers chung =========
def _safe_int(x):
    try:
        return int(x)
    except Exception:
        return 0

def _mode_text(series):
    vals = [str(x).strip() for x in series if str(x).strip() and str(x).strip().lower() != "nan"]
    if not vals:
        return ""
    cnt = Counter(vals)
    mx = max(cnt.values())
    # ổn định thứ tự: tên A trước B khi đồng hạng
    return sorted([v for v, c in cnt.items() if c == mx])[0]

# ========= BƯỚC 1: CẮT FILE BẰNG STREAMING =========
def stream_cut_excel(file_bytes: bytes,
                     sheet_name: Optional[str] = None,
                     col_indices: List[int] = COL_INDICES,
                     preserve_all_rows: bool = True) -> bytes:
    """
    Đọc Excel bằng openpyxl streaming, chỉ lấy các cột theo index (0-based).
    - Không load toàn bộ file vào RAM.
    - Giữ nguyên số dòng (kể cả dòng trống) nếu preserve_all_rows=True.
    Trả về bytes nội dung .xlsx đã cắt (8 cột).
    """
    from openpyxl import load_workbook
    from openpyxl.writer.excel import save_virtual_workbook
    from openpyxl.workbook import Workbook

    # 1) Mở workbook nguồn ở chế độ read-only
    bio = BytesIO(file_bytes)
    wb_src = load_workbook(bio, read_only=True, data_only=True)
    ws_src = wb_src[sheet_name] if sheet_name and sheet_name in wb_src.sheetnames else wb_src.active

    # 2) Tạo workbook đích (write-only → nhẹ RAM)
    from openpyxl import Workbook
    wb_out = Workbook(write_only=True)
    ws_out = wb_out.create_sheet(title=ws_src.title)

    # 3) Duyệt từng dòng, trích cột theo index
    for row in ws_src.iter_rows(values_only=True):
        # đảm bảo đủ độ dài
        row = list(row)
        if len(row) <= max(col_indices):
            # hàng này thiếu cột ở đuôi → fill None
            row = row + [None] * (max(col_indices) - len(row) + 1)
        new_row = [row[i] for i in col_indices]
        if preserve_all_rows:
            ws_out.append(new_row)
        else:
            # (tuỳ chọn) bỏ dòng trống hoàn toàn
            if any(cell is not None and str(cell).strip() != "" for cell in new_row):
                ws_out.append(new_row)

    # 4) Xuất bytes
    out_bytes = save_virtual_workbook(wb_out)
    return out_bytes

# ========= BƯỚC 2: PIVOT THEO KH =========
def normalize_after_cut(df: pd.DataFrame, header_row_user: int) -> pd.DataFrame:
    """
    Chuẩn hoá tên cột sau khi cắt.
    - header_row_user: hàng tiêu đề (1-based) mà người dùng chọn trong file đã cắt.
    - Nếu không đủ tên ⇒ đặt tên theo INDEX_TO_REQUIRED.
    """
    # Nếu người dùng chọn header ở dòng khác 1 → đọc lại với header phù hợp
    if header_row_user != 1:
        # chuyển DataFrame hiện tại thành Excel bytes rồi đọc lại với header=header_row_user-1
        buf = BytesIO()
        df.to_excel(buf, index=False, header=False)
        buf.seek(0)
        df = pd.read_excel(buf, header=header_row_user - 1)

    # Chuẩn hoá tên về str
    df.columns = [str(c).strip() for c in df.columns]

    # Nếu thiếu REQUIRED → thử map theo index
    need_rename = {}
    for idx, col in enumerate(df.columns[:len(INDEX_TO_REQUIRED)]):
        target = INDEX_TO_REQUIRED.get(idx)
        if target:
            need_rename[col] = target
    df = df.rename(columns=need_rename)

    # Giữ đúng các cột bắt buộc (nếu thiếu thì tạo rỗng)
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
    - Cột động theo Tên SP (sản lượng)
    - Cột 'Tổng Doanh số'
    - Thêm 2 cột đại diện: Tên KH, Tên NPP (mode)
    """
    rep = (
        df.groupby("Mã KH")
          .agg(**{
              "Tên KH đại diện": ("Tên KH", _mode_text),
              "Tên NPP đại diện": ("Tên NPP", _mode_text),
          }).reset_index()
    )

    qty = pd.pivot_table(
        df, index=["Mã KH"], columns="Tên SP",
        values="Tổng Sản lượng (Lẻ)", aggfunc="sum", fill_value=0, observed=False
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
st.header("Bước 1 — ✂️ Cắt cột từ file nặng (streaming, không mất dòng)")
with st.expander("Chọn & cắt file", expanded=True):
    c1, c2 = st.columns([2,1])
    with c1:
        raw_file = st.file_uploader("Upload file Excel GỐC (có thể rất lớn)", type=["xlsx", "xlsm", "xlsb", "xls"])
    with c2:
        sheet_hint = st.text_input("Tên sheet (bỏ trống = sheet đầu tiên)", value="")

    if raw_file:
        # Tăng giới hạn upload nếu deploy tại chỗ (tuỳ chọn)
        # st.set_option("server.maxUploadSize", 500)  # MB
        with st.spinner("Đang cắt cột bằng streaming..."):
            trimmed_bytes = stream_cut_excel(
                file_bytes=raw_file.read(),
                sheet_name=sheet_hint or None,
                col_indices=COL_INDICES,
                preserve_all_rows=True,  # giữ nguyên số dòng
            )
        st.success("✅ Đã cắt xong. Bạn có thể tải về để kiểm tra hoặc dùng trực tiếp cho Bước 2.")
        st.download_button(
            "⬇️ Tải file đã cắt (.xlsx)",
            data=trimmed_bytes,
            file_name=f"{os.path.splitext(raw_file.name)[0]}_filtered_preserve.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
        st.session_state["trimmed_bytes"] = trimmed_bytes

# =========================
# UI — Bước 2: PIVOT
# =========================
st.header("Bước 2 — 📊 Pivot sản lượng & doanh số theo Khách Hàng")

src_choice = st.radio(
    "Chọn nguồn file ‘đã cắt’ để pivot:",
    ["Upload mới", "Dùng file đã cắt ở Bước 1"],
    horizontal=True
)

trimmed_to_use = None
if src_choice == "Upload mới":
    trimmed_upload = st.file_uploader("Upload file ĐÃ CẮT (8 cột)", type=["xlsx"], key="trimmed_upload")
    if trimmed_upload:
        trimmed_to_use = trimmed_upload.read()
else:
    if "trimmed_bytes" in st.session_state:
        trimmed_to_use = st.session_state["trimmed_bytes"]
    else:
        st.info("Chưa có file từ Bước 1. Vui lòng upload mới.")

# Chọn dòng tiêu đề (nếu header trong file không nằm ở dòng 1)
header_row_user = st.number_input(
    "Dòng tiêu đề trong file đã cắt (1 = dòng đầu)", min_value=1, value=1, step=1
)

if trimmed_to_use and st.button("🚀 Pivot ngay", use_container_width=True):
    try:
        with st.spinner("Đang đọc & chuẩn hoá dữ liệu..."):
            # Đọc toàn bộ sheet đầu tiên
            df_cut = pd.read_excel(BytesIO(trimmed_to_use), header=None, dtype=object, engine="openpyxl")
            # Đặt tên tạm theo index nếu không có header
            if header_row_user == 1:
                # đọc lại với header=0 để giữ hàng đầu tiên làm tên cột (nếu có)
                df_cut_named = pd.read_excel(BytesIO(trimmed_to_use), header=0, dtype=object, engine="openpyxl")
            else:
                # giữ nguyên, normalize_after_cut sẽ đọc lại theo header_row_user
                df_cut_named = df_cut

            df_norm = normalize_after_cut(df_cut_named, header_row_user=header_row_user)

            # Kiểm tra thiếu cột thiết yếu
            miss = [c for c in REQUIRED if c not in df_norm.columns]
            if miss:
                st.error(f"Thiếu cột bắt buộc: {miss}")
                st.stop()

        with st.spinner("Đang pivot theo Khách Hàng..."):
            pivot_df = build_pivot_by_customer(df_norm)

        st.success("✅ Xong! Dưới đây là bảng pivot:")
        st.dataframe(pivot_df, use_container_width=True)

        # Tải Excel
        out_buf = BytesIO()
        with pd.ExcelWriter(out_buf, engine="openpyxl") as w:
            pivot_df.to_excel(w, index=False, sheet_name="PIVOT_KH")
        st.download_button(
            "⬇️ Tải Excel PIVOT",
            data=out_buf.getvalue(),
            file_name="pivot_sku_theo_khachhang.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

        with st.expander("⚙️ Tùy chọn lọc nhanh"):
            col1, col2 = st.columns(2)
            with col1:
                kw = st.text_input("Tìm theo Mã KH / Tên KH / NPP", "")
            with col2:
                min_rev = st.number_input("Lọc Tổng Doanh số ≥", min_value=0, value=0, step=50_000)

            filt = pivot_df.copy()
            if kw:
                k = kw.lower().strip()
                cols_search = [c for c in filt.columns if c in ["Mã KH","Tên KH đại diện","Tên NPP đại diện"]]
                mask = False
                for c in cols_search:
                    mask = mask | filt[c].astype(str).str.lower().str.contains(k)
                filt = filt[mask]
            if "Tổng Doanh số" in filt.columns:
                filt = filt[pd.to_numeric(filt["Tổng Doanh số"], errors="coerce").fillna(0) >= min_rev]

            st.dataframe(filt, use_container_width=True)

    except Exception as e:
        st.error(f"Lỗi khi pivot: {e}")
        st.exception(e)
