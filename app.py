# app.py  —  Trim (2 chế độ) + Pivot SKU theo Khách hàng
import os
from io import BytesIO
from collections import Counter
from typing import Optional

import pandas as pd
import streamlit as st

# ─────────────────────────────────────────────
# Cấu hình trang
# ─────────────────────────────────────────────
st.set_page_config(page_title="CẮT & PIVOT SKU", layout="wide")
st.title("✂️ CẮT FILE & 📊 PIVOT SẢN LƯỢNG/DOANH SỐ THEO KHÁCH HÀNG")
st.caption("Hai chế độ: (1) Trực tiếp ≤200 MB  •  (2) Ngoại tuyến >200 MB")

# Cột cần lấy (theo chữ cái Excel)
USECOLS_LETTERS = "D,L,M,Q,R,S,W,Z"
REQUIRED = [
    "Tên NPP", "Mã KH", "Tên KH", "Nhóm hàng",
    "Mã SP", "Tên SP", "Tổng Sản lượng (Lẻ)", "Doanh số bán"
]
INDEX_TO_REQUIRED = {
    0: "Tên NPP",
    1: "Mã KH",
    2: "Tên KH",
    3: "Nhóm hàng",
    4: "Mã SP",
    5: "Tên SP",
    6: "Tổng Sản lượng (Lẻ)",
    7: "Doanh số bán",
}

# ─────────────────────────────────────────────
# Helpers chung
# ─────────────────────────────────────────────
def _mode_text(series):
    vals = [str(x).strip() for x in series if str(x).strip() and str(x).strip().lower() != "nan"]
    if not vals:
        return ""
    cnt = Counter(vals)
    mx = max(cnt.values())
    return sorted([v for v, c in cnt.items() if c == mx])[0]

def normalize_after_cut(df: pd.DataFrame, header_row_user: int) -> pd.DataFrame:
    """Chuẩn hoá tên cột & kiểu dữ liệu sau khi cắt."""
    if header_row_user != 1:
        buf = BytesIO()
        df.to_excel(buf, index=False, header=False)
        buf.seek(0)
        df = pd.read_excel(buf, header=header_row_user - 1, engine="openpyxl")

    df.columns = [str(c).strip() for c in df.columns]

    # map theo vị trí
    ren = {}
    for i, c in enumerate(df.columns[:len(INDEX_TO_REQUIRED)]):
        target = INDEX_TO_REQUIRED.get(i)
        if target:
            ren[c] = target
    df = df.rename(columns=ren)

    # đảm bảo đủ cột
    for c in REQUIRED:
        if c not in df.columns:
            df[c] = None
    df = df[REQUIRED].copy()

    # kiểu số
    df["Tổng Sản lượng (Lẻ)"] = pd.to_numeric(df["Tổng Sản lượng (Lẻ)"], errors="coerce").fillna(0)
    df["Doanh số bán"] = pd.to_numeric(df["Doanh số bán"], errors="coerce").fillna(0)

    # chuẩn chuỗi
    for c in ["Tên NPP", "Mã KH", "Tên KH", "Nhóm hàng", "Mã SP", "Tên SP"]:
        df[c] = df[c].astype(str).str.strip()

    return df

def build_pivot_by_customer(df: pd.DataFrame) -> pd.DataFrame:
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
    if "Tổng Doanh số" in cols: dynamic.append("Tổng Doanh số")
    return out[fixed + dynamic]

# ─────────────────────────────────────────────
# CẮT: 2 phương án
# ─────────────────────────────────────────────
def cut_pandas_usecols(file_bytes: bytes, sheet_name: Optional[str]) -> bytes:
    """
    NHANH (≤200MB): chỉ đọc đúng 8 cột bằng pandas `usecols="D,L,M,Q,R,S,W,Z"`.
    """
    bio = BytesIO(file_bytes)
    xls = pd.ExcelFile(bio, engine="openpyxl")
    real_sheet = sheet_name if (sheet_name and sheet_name in xls.sheet_names) else xls.sheet_names[0]

    df_cut = pd.read_excel(
        xls, sheet_name=real_sheet, engine="openpyxl",
        dtype=object, keep_default_na=False, usecols=USECOLS_LETTERS
    )
    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        df_cut.to_excel(w, index=False, sheet_name="Trimmed")
    return out.getvalue()

def cut_openpyxl_streaming(file_bytes: bytes, sheet_name: Optional[str]) -> bytes:
    """
    SIÊU TIẾT KIỆM RAM: đọc/ghi streaming bằng openpyxl (từng hàng).
    """
    from openpyxl import load_workbook, Workbook
    from openpyxl.utils import column_index_from_string

    idxs = [column_index_from_string(c) for c in USECOLS_LETTERS.split(",")]  # 1-based

    wb_in = load_workbook(BytesIO(file_bytes), read_only=True, data_only=True)
    ws_in = wb_in[sheet_name] if (sheet_name and sheet_name in wb_in.sheetnames) else wb_in.active

    wb_out = Workbook(write_only=True)
    ws_out = wb_out.create_sheet(title="Trimmed")

    for row in ws_in.iter_rows(values_only=True):
        out_row = []
        for i in idxs:
            v = row[i-1] if (i-1) < len(row) else None
            out_row.append(v)
        ws_out.append(out_row)

    buf = BytesIO()
    wb_out.save(buf)  # save() cho write_only workbook
    return buf.getvalue()

# ─────────────────────────────────────────────
# UI: Hai luồng xử lý
# ─────────────────────────────────────────────
tab_small, tab_big = st.tabs(["🔹 Trực tiếp (≤ 200 MB)", "🔸 Ngoại tuyến (> 200 MB)"])

# ---------- TAB 1: TRỰC TIẾP ----------
with tab_small:
    st.subheader("Bước 1 — CẮT cột trực tiếp (≤200 MB)")
    c1, c2 = st.columns([2,1])
    with c1:
        raw_file = st.file_uploader(
            "Upload file Excel GỐC (xlsx/xlsm; nếu xls/xlsb hãy Save As → .xlsx trước)",
            type=["xlsx", "xlsm", "xls"], key="raw_small"
        )
    with c2:
        sheet_name = None
        if raw_file:
            try:
                xls = pd.ExcelFile(raw_file, engine="openpyxl")
                sheet_name = st.selectbox("Chọn sheet", xls.sheet_names, key="sheet_small")
            except Exception as e:
                st.warning(f"Không đọc được danh sách sheet (sẽ dùng sheet đầu tiên). Chi tiết: {e}")
                sheet_name = None

    mode = st.radio(
        "Cách cắt:",
        ["Nhanh (pandas)", "Siêu tiết kiệm RAM (openpyxl streaming)"],
        horizontal=True, key="mode_small"
    )

    if raw_file and st.button("✂️ CẮT NGAY", use_container_width=True, key="cut_small_btn"):
        try:
            with st.spinner("Đang cắt..."):
                if mode.startswith("Nhanh"):
                    trimmed = cut_pandas_usecols(raw_file.getvalue(), sheet_name)
                else:
                    trimmed = cut_openpyxl_streaming(raw_file.getvalue(), sheet_name)
            st.session_state["trimmed_bytes"] = trimmed
            st.success("✅ Đã cắt xong.")
            st.download_button(
                "⬇️ Tải file đã cắt (.xlsx)",
                data=trimmed,
                file_name=f"{os.path.splitext(raw_file.name)[0]}_trimmed.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="dl_trim_small",
            )
        except Exception as e:
            st.error(f"Lỗi khi cắt: {e}")

    st.markdown("---")
    st.subheader("Bước 2 — Pivot theo Khách Hàng")

    source_choice = st.radio(
        "Nguồn file pivot:",
        ["Dùng file đã cắt ở Bước 1", "Upload file đã cắt (8 cột)"],
        horizontal=True, key="src_small"
    )
    trimmed_to_use = None
    if source_choice.startswith("Dùng"):
        if "trimmed_bytes" in st.session_state:
            trimmed_to_use = st.session_state["trimmed_bytes"]
        else:
            st.info("Chưa có file đã cắt trong session. Hãy cắt ở trên hoặc chuyển sang ‘Upload file đã cắt’.")
    else:
        up2 = st.file_uploader("Upload file đã cắt (8 cột)", type=["xlsx"], key="trimmed_small")
        if up2:
            trimmed_to_use = up2.getvalue()

    header_row = st.number_input("Dòng tiêu đề (1 = dòng đầu)", min_value=1, value=1, step=1, key="hdr_small")

    if trimmed_to_use and st.button("🚀 PIVOT NGAY", use_container_width=True, key="pivot_small_btn"):
        try:
            with st.spinner("Đang đọc & chuẩn hoá..."):
                df_cut = pd.read_excel(BytesIO(trimmed_to_use), header=0, engine="openpyxl")
                df_norm = normalize_after_cut(df_cut, header_row_user=header_row)

                miss = [c for c in REQUIRED if c not in df_norm.columns]
                if miss:
                    st.error(f"Thiếu cột bắt buộc: {miss}")
                    st.stop()

            with st.spinner("Đang pivot..."):
                pivot_df = build_pivot_by_customer(df_norm)

            st.success("✅ Xong!")
            st.dataframe(pivot_df, use_container_width=True)

            out = BytesIO()
            with pd.ExcelWriter(out, engine="openpyxl") as w:
                pivot_df.to_excel(w, index=False, sheet_name="PIVOT_KH")
            st.download_button(
                "⬇️ Tải Excel PIVOT",
                data=out.getvalue(),
                file_name="pivot_sku_theo_khachhang.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="dl_pivot_small",
            )
        except Exception as e:
            st.error(f"Lỗi pivot: {e}")

# ---------- TAB 2: NGOẠI TUYẾN ----------
with tab_big:
    st.subheader("Khi file gốc > 200 MB (không upload được)")
    st.markdown(
        """
        **Cách làm:**
        1) Tải script cắt **ngoại tuyến** (dùng openpyxl streaming).
        2) Chạy script trên máy: chọn file gốc & sheet → script tạo file **_trimmed.xlsx** (chỉ 8 cột).
        3) Quay lại tab **Trực tiếp** hoặc phần dưới đây để **Upload file đã cắt** và Pivot.

        **Yêu cầu Python cục bộ:** `pip install openpyxl`
        """
    )

    # Nội dung script ngoại tuyến:
    offline_script = f"""# cutter_offline.py - Cắt 8 cột (D,L,M,Q,R,S,W,Z) bằng streaming
from io import BytesIO
from openpyxl import load_workbook, Workbook
from openpyxl.utils import column_index_from_string
import os

USECOLS = "{USECOLS_LETTERS}".split(",")

def cut_streaming(path_in: str, sheet_name: str = None) -> str:
    idxs = [column_index_from_string(c) for c in USECOLS]
    with open(path_in, "rb") as f:
        data = f.read()

    wb_in = load_workbook(BytesIO(data), read_only=True, data_only=True)
    ws_in = wb_in[sheet_name] if (sheet_name and sheet_name in wb_in.sheetnames) else wb_in.active

    wb_out = Workbook(write_only=True)
    ws_out = wb_out.create_sheet(title="Trimmed")

    for row in ws_in.iter_rows(values_only=True):
        out_row = []
        for i in idxs:
            v = row[i-1] if (i-1) < len(row) else None
            out_row.append(v)
        ws_out.append(out_row)

    out_path = os.path.splitext(path_in)[0] + "_trimmed.xlsx"
    wb_out.save(out_path)
    return out_path

if __name__ == "__main__":
    path = input("Đường dẫn file Excel gốc (.xlsx): ").strip().strip('"')
    sheet = input("Tên sheet (Enter = sheet đầu tiên): ").strip() or None
    out = cut_streaming(path, sheet)
    print("Đã xuất:", out)
"""

    # Cho tải script
    st.download_button(
        "⬇️ Tải script cắt ngoại tuyến (cutter_offline.py)",
        data=offline_script.encode("utf-8"),
        file_name="cutter_offline.py",
        mime="text/x-python",
        use_container_width=True,
        key="dl_offline_script",
    )

    st.markdown("---")
    st.subheader("Pivot ngay từ file đã cắt (upload ở đây)")
    up_big = st.file_uploader("Upload file *_trimmed.xlsx", type=["xlsx"], key="trimmed_big")
    header_row_big = st.number_input("Dòng tiêu đề (1 = dòng đầu)", min_value=1, value=1, step=1, key="hdr_big")

    if up_big and st.button("🚀 PIVOT (file đã cắt)", use_container_width=True, key="pivot_big_btn"):
        try:
            df_cut = pd.read_excel(up_big, header=0, engine="openpyxl")
            df_norm = normalize_after_cut(df_cut, header_row_user=header_row_big)
            miss = [c for c in REQUIRED if c not in df_norm.columns]
            if miss:
                st.error(f"Thiếu cột bắt buộc: {miss}")
                st.stop()
            pivot_df = build_pivot_by_customer(df_norm)

            st.success("✅ Xong!")
            st.dataframe(pivot_df, use_container_width=True)

            out = BytesIO()
            with pd.ExcelWriter(out, engine="openpyxl") as w:
                pivot_df.to_excel(w, index=False, sheet_name="PIVOT_KH")
            st.download_button(
                "⬇️ Tải Excel PIVOT",
                data=out.getvalue(),
                file_name="pivot_sku_theo_khachhang.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="dl_pivot_big",
            )
        except Exception as e:
            st.error(f"Lỗi pivot: {e}")
