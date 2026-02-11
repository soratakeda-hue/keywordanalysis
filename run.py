#!/usr/bin/env python3
import argparse
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

import analyzer
import config


# =====================
# CLI
# =====================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, default=Path("data"))
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    return parser.parse_args()

def _sanitize_filename_component(s: str) -> str:
    """
    ファイル名に使いづらい文字を安全な文字に置換する（OS差異の吸収）。
    """
    s = str(s)
    invalid = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
    for ch in invalid:
        s = s.replace(ch, "_")
    return s.strip()


def _make_unique_path(path: Path) -> Path:
    """
    同名ファイルが存在する場合、末尾に _2, _3... を付与してユニークなパスを返す。
    """
    if not path.exists():
        return path

    suffix = path.suffix
    stem = path.stem
    for n in range(2, 1000):
        candidate = path.with_name(f"{stem}_{n}{suffix}")
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"Failed to generate unique output path for: {path}")


# =====================
# Column definitions
# =====================

NOW_COLUMNS = [
    "keyword",
    "imp", "click", "cost", "cv",
    "ctr", "cvr", "cpc", "cpa",
    "avg_position",
    "imp_share", "click_share", "cost_share", "cv_share",
]

# keyword を削除
DELTA_COLUMNS = [
    "delta_imp_share",
    "delta_click_share",
    "delta_cost_share",
    "delta_cv_share",
]

SORT_PATTERNS = [
    ("COST降順", "cost", False),
    ("CV降順", "cv", False),
    ("CPA昇順", "cpa", True),
]

PERCENT_COLUMNS = {
    "ctr", "cvr",
    "imp_share", "click_share", "cost_share", "cv_share",
    "delta_imp_share", "delta_click_share", "delta_cost_share", "delta_cv_share",
}

# カラム名の日本語マッピング
COLUMN_NAMES_JP = {
    "keyword": "キーワード",
    "imp": "表示回数",
    "click": "クリック数",
    "cost": "費用",
    "cv": "応募数",
    "ctr": "CTR",
    "cvr": "CVR",
    "cpc": "CPC",
    "cpa": "CPA",
    "avg_position": "平均表示順位",
    "imp_share": "表示回数シェア",
    "click_share": "クリック数シェア",
    "cost_share": "費用シェア",
    "cv_share": "応募数シェア",
}


# =====================
# Excel writer helper
# =====================

def write_sheet_now_only(
    writer: pd.ExcelWriter,
    sheet_name: str,
    now_df: pd.DataFrame,
    avg_cpa: float = None,  # 平均CPA（Noneの場合はnow_dfから計算）
    grouping_rules: list = None,  # 追加（デフォルト値で後方互換性を維持）
):
    """今期データのみを出力するシート"""
    workbook = writer.book

    # Excelのシート名は「31文字制限」「大小文字を無視した重複禁止」。
    # 通常の命名は維持しつつ、衝突時のみ `{base}_{period}_{n}` 形式で末尾採番して回避する。
    MAX_SHEETNAME_LEN = 31

    def _casefold(s: str) -> str:
        return str(s).casefold()

    def _existing_sheet_names_casefolded() -> set[str]:
        # pandas の ExcelWriter は既存シートを writer.sheets に保持する
        return {_casefold(name) for name in getattr(writer, "sheets", {}).keys()}

    def _ensure_len(name: str, max_len: int) -> str:
        name = str(name)
        return name if len(name) <= max_len else name[:max_len]

    def _split_trailing_period(name: str) -> Optional[Tuple[str, str]]:
        """
        末尾が `_<4桁><-><4桁>`（例: `_1201-1231`）の場合に (base, period) を返す。
        それ以外は None。
        """
        name = str(name)
        if len(name) < 10:
            return None
        if name[-10] != "_":
            return None
        period = name[-9:]
        if len(period) != 9 or period[4] != "-":
            return None
        if not (period[:4].isdigit() and period[5:].isdigit()):
            return None
        base = name[:-10]
        return (base, period)

    def _normalize_requested_name(requested: str) -> str:
        """
        31文字制限に収める。期間形式が末尾にある場合は period を残し、base 側を切り詰める。
        """
        requested = str(requested)
        split = _split_trailing_period(requested)
        if split is None:
            return _ensure_len(requested, MAX_SHEETNAME_LEN)
        base, period = split
        max_base_len = MAX_SHEETNAME_LEN - (1 + len(period))  # '_' + period
        if max_base_len < 0:
            # 念のため（ここに来ることはほぼない）
            return _ensure_len(requested, MAX_SHEETNAME_LEN)
        return f"{_ensure_len(base, max_base_len)}_{period}"

    def _make_unique_sheet_name(requested: str) -> str:
        requested = _normalize_requested_name(requested)
        used = _existing_sheet_names_casefolded()

        if _casefold(requested) not in used:
            return requested

        # 末尾に _2, _3... を付ける（大小文字無視で重複チェック）
        for n in range(2, 1000):
            suffix = f"_{n}"
            # 末尾採番を必ず残し、31文字を超える場合は base 側（前方）を切り詰める
            split = _split_trailing_period(requested)
            if split is None:
                prefix_max = MAX_SHEETNAME_LEN - len(suffix)
                candidate = _ensure_len(requested, prefix_max) + suffix
            else:
                base, period = split
                max_base_len = MAX_SHEETNAME_LEN - (1 + len(period) + len(suffix))  # '_' + period + suffix
                candidate = f"{_ensure_len(base, max_base_len)}_{period}{suffix}"
            if _casefold(candidate) not in used:
                return candidate

        raise RuntimeError(f"Failed to generate unique sheet name for: {requested!r}")

    actual_sheet_name = _make_unique_sheet_name(sheet_name)
    worksheet = workbook.add_worksheet(actual_sheet_name)
    writer.sheets[actual_sheet_name] = worksheet

    percent_fmt = workbook.add_format({"num_format": "0.0%"})
    number_fmt = workbook.add_format({"num_format": "#,##0"})
    currency_fmt = workbook.add_format({"num_format": "¥#,##0"})
    position_fmt = workbook.add_format({"num_format": "0.0"})
    header_fmt = workbook.add_format({"bold": True, "bg_color": "#D3D3D3"})
    
    # 平均CPAを計算（引数がNoneの場合はnow_dfから計算）
    # 加重平均を使用（CV>0の行のみで計算）
    if avg_cpa is None:
        valid_mask = now_df["cv"] > 0
        if valid_mask.sum() > 0:
            total_cost = now_df.loc[valid_mask, "cost"].sum()
            total_cv = now_df.loc[valid_mask, "cv"].sum()
            avg_cpa = total_cost / total_cv if total_cv > 0 else 0
        else:
            avg_cpa = 0
    
    current_row = 0
    
    # 補足説明（凡例）を追加
    worksheet.write(current_row, 0, "CPA列の色分け：", header_fmt)
    current_row += 1
    
    # 平均CPAの値を表示
    worksheet.write(current_row, 0, "  平均CPA:", header_fmt)
    if avg_cpa > 0:
        worksheet.write(current_row, 1, avg_cpa, currency_fmt)
    else:
        worksheet.write(current_row, 1, "計算不可（CV=0のみ）")
    current_row += 1
    current_row += 1  # 空行を追加（条件説明との間のスペース）
    
    worksheet.write(current_row, 0, "  平均より-10%未満（良い）: 濃い緑")
    worksheet.write(current_row, 1, "", workbook.add_format({"bg_color": "#C6EFCE"}))  # 色見本
    current_row += 1
    worksheet.write(current_row, 0, "  平均より-10%～-5%（やや良い）: 薄い緑")
    worksheet.write(current_row, 1, "", workbook.add_format({"bg_color": "#E2EFDA"}))  # 色見本
    current_row += 1
    worksheet.write(current_row, 0, "  平均より-5%～+5%（標準）: 白")
    current_row += 1
    worksheet.write(current_row, 0, "  平均より+5%～+10%（やや悪い）: 薄い赤")
    worksheet.write(current_row, 1, "", workbook.add_format({"bg_color": "#FFC7CE"}))  # 色見本
    current_row += 1
    worksheet.write(current_row, 0, "  平均より+10%超（悪い）: 濃い赤")
    worksheet.write(current_row, 1, "", workbook.add_format({"bg_color": "#FF6B6B"}))  # 色見本
    current_row += 1
    worksheet.write(current_row, 0, "  CPA=0（CV獲得ゼロ）: グレー")
    worksheet.write(current_row, 1, "", workbook.add_format({"bg_color": "#D3D3D3"}))  # 色見本
    current_row += 2  # 空行を追加

    current_col = 0

    for i, (title, sort_key, ascending) in enumerate(SORT_PATTERNS):
        # ---- title ----
        worksheet.write(current_row, current_col, title)

        # ---- sort ----
        # CPA昇順の場合、CVが0またはCPAが0/NaNのものは後ろに回す
        if sort_key == "cpa" and ascending:
            # CVが0またはCPAが0/NaNのものとそうでないものを分ける
            mask_valid = (now_df["cv"] > 0) & (now_df["cpa"] > 0) & (now_df["cpa"].notna())
            df_valid = now_df[mask_valid].sort_values(sort_key, ascending=ascending)
            df_invalid = now_df[~mask_valid]
            now_sorted = pd.concat([df_valid, df_invalid])
        else:
            now_sorted = now_df.sort_values(sort_key, ascending=ascending)

        # ---- 今期データ（日本語ヘッダー付き）----
        now_data = now_sorted.reset_index()[NOW_COLUMNS].copy()
        now_data.columns = [COLUMN_NAMES_JP.get(col, col) for col in now_data.columns]
        now_data.to_excel(
            writer,
            sheet_name=actual_sheet_name,
            startrow=current_row + 1,  # 補足説明とタイトル行の後
            startcol=current_col,
            index=False,
        )

        # ---- フォーマット設定 ----
        for offset, col in enumerate(NOW_COLUMNS):
            col_idx = current_col + offset
            if col in PERCENT_COLUMNS:
                worksheet.set_column(col_idx, col_idx, None, percent_fmt)
            elif col == "avg_position":
                worksheet.set_column(col_idx, col_idx, None, position_fmt)
            elif col in ["imp", "click", "cv"]:
                worksheet.set_column(col_idx, col_idx, None, number_fmt)
            elif col in ["cost", "cpc", "cpa"]:  # cpcとcpaも追加
                worksheet.set_column(col_idx, col_idx, None, currency_fmt)

        # ---- 条件付き書式設定（CPA列） ----
        # CPA列のインデックスを取得
        cpa_col_idx = current_col + NOW_COLUMNS.index("cpa")
        
        # データ行の範囲を取得（ヘッダー行を除く）
        data_start_row = current_row + 2  # 補足説明の行数 + タイトル行 + ヘッダー行
        data_end_row = data_start_row + len(now_sorted) - 1
        
        # 色のフォーマットを作成
        dark_green_fmt = workbook.add_format({"bg_color": "#C6EFCE"})
        light_green_fmt = workbook.add_format({"bg_color": "#E2EFDA"})
        light_red_fmt = workbook.add_format({"bg_color": "#FFC7CE"})
        dark_red_fmt = workbook.add_format({"bg_color": "#FF6B6B"})
        gray_fmt = workbook.add_format({"bg_color": "#D3D3D3"})  # CPA=0用のグレー
        
        # 基準値（平均CPA）
        base_cpa = avg_cpa
        
        # 条件付き書式を適用
        if base_cpa > 0:
            # 平均CPAが0より大きい場合、5段階の条件付き書式を適用
            # 最初にCPA=0の条件を適用（優先度を高めるため）
            worksheet.conditional_format(
                data_start_row, cpa_col_idx, data_end_row, cpa_col_idx,
                {
                    "type": "cell",
                    "criteria": "==",
                    "value": 0,
                    "format": gray_fmt,
                }
            )
            
            # 1. 平均の-10%未満（ただし0より大きい）= 濃い緑
            worksheet.conditional_format(
                data_start_row, cpa_col_idx, data_end_row, cpa_col_idx,
                {
                    "type": "cell",
                    "criteria": "between",
                    "minimum": 0.0001,  # 0より大きい値（非常に小さい値）
                    "maximum": base_cpa * 0.9,  # 平均の90%（-10%）
                    "format": dark_green_fmt,
                }
            )
            # 2. 平均の-10%～-5% = 薄い緑
            worksheet.conditional_format(
                data_start_row, cpa_col_idx, data_end_row, cpa_col_idx,
                {
                    "type": "cell",
                    "criteria": "between",
                    "minimum": base_cpa * 0.9,  # 平均の90%（-10%）
                    "maximum": base_cpa * 0.95,  # 平均の95%（-5%）
                    "format": light_green_fmt,
                }
            )
            # 3. 平均の-5%～+5% = 白（デフォルトのため条件付き書式は不要）
            # 4. 平均の+5%～+10% = 薄い赤
            worksheet.conditional_format(
                data_start_row, cpa_col_idx, data_end_row, cpa_col_idx,
                {
                    "type": "cell",
                    "criteria": "between",
                    "minimum": base_cpa * 1.05,  # 平均の105%（+5%）
                    "maximum": base_cpa * 1.1,  # 平均の110%（+10%）
                    "format": light_red_fmt,
                }
            )
            # 5. 平均の+10%超 = 濃い赤
            worksheet.conditional_format(
                data_start_row, cpa_col_idx, data_end_row, cpa_col_idx,
                {
                    "type": "cell",
                    "criteria": ">",
                    "value": base_cpa * 1.1,  # 平均の110%（+10%）
                    "format": dark_red_fmt,
                }
            )
        else:
            # 平均CPAが0の場合の処理
            # CPA=0以外はすべて濃い赤（平均が計算できないため）
            worksheet.conditional_format(
                data_start_row, cpa_col_idx, data_end_row, cpa_col_idx,
                {
                    "type": "cell",
                    "criteria": ">",
                    "value": 0,
                    "format": dark_red_fmt,
                }
            )
            # CPA=0はグレー
            worksheet.conditional_format(
                data_start_row, cpa_col_idx, data_end_row, cpa_col_idx,
                {
                    "type": "cell",
                    "criteria": "==",
                    "value": 0,
                    "format": gray_fmt,
                }
            )

        # ---- 次ブロック開始位置 ----
        current_col += len(NOW_COLUMNS)

        # ---- SORTブロック間の空列 ----
        if i < len(SORT_PATTERNS) - 1:
            worksheet.write(current_row + 1, current_col, "")  # current_row + 1に変更
            current_col += 1


def write_sheet_with_3sort_blocks(
    writer: pd.ExcelWriter,
    sheet_name: str,
    now_df: pd.DataFrame,
    delta_df: pd.DataFrame,
):
    workbook = writer.book
    worksheet = workbook.add_worksheet(sheet_name)
    writer.sheets[sheet_name] = worksheet

    percent_fmt = workbook.add_format({"num_format": "0.0%"})

    current_col = 0

    for i, (title, sort_key, ascending) in enumerate(SORT_PATTERNS):
        # ---- title ----
        worksheet.write(0, current_col, title)

        # ---- sort ----
        # CPA昇順の場合、CVが0またはCPAが0/NaNのものは後ろに回す
        if sort_key == "cpa" and ascending:
            # CVが0またはCPAが0/NaNのものとそうでないものを分ける
            mask_valid = (now_df["cv"] > 0) & (now_df["cpa"] > 0) & (now_df["cpa"].notna())
            df_valid = now_df[mask_valid].sort_values(sort_key, ascending=ascending)
            df_invalid = now_df[~mask_valid]
            now_sorted = pd.concat([df_valid, df_invalid])
            delta_sorted = delta_df.loc[now_sorted.index]
        else:
            now_sorted = now_df.sort_values(sort_key, ascending=ascending)
            delta_sorted = delta_df.loc[now_sorted.index]

        # ---- 今期単体 ----
        now_sorted.reset_index()[NOW_COLUMNS].to_excel(
            writer,
            sheet_name=sheet_name,
            startrow=1,
            startcol=current_col,
            index=False,
        )

        # ---- 前期対比（keywordなし）----
        delta_start_col = current_col + len(NOW_COLUMNS)
        delta_sorted[DELTA_COLUMNS].to_excel(
            writer,
            sheet_name=sheet_name,
            startrow=1,
            startcol=delta_start_col,
            index=False,
        )

        # ---- %表示設定 ----
        for offset, col in enumerate(NOW_COLUMNS):
            if col in PERCENT_COLUMNS:
                worksheet.set_column(current_col + offset, current_col + offset, None, percent_fmt)

        for offset, col in enumerate(DELTA_COLUMNS):
            if col in PERCENT_COLUMNS:
                worksheet.set_column(delta_start_col + offset, delta_start_col + offset, None, percent_fmt)

        # ---- 次ブロック開始位置 ----
        current_col = delta_start_col + len(DELTA_COLUMNS)

        # ---- SORTブロック間の空列 ----
        if i < len(SORT_PATTERNS) - 1:
            worksheet.write(1, current_col, "")
            current_col += 1


def write_summary_sheet(
    writer: pd.ExcelWriter,
    sheet_name: str,
    account_name: str,
    period_a_str: str,
    period_b_str: str,
    totals_a: analyzer.TotalsData,
    totals_b: analyzer.TotalsData,
    campaign_totals_a: dict[str, analyzer.TotalsData],
    campaign_totals_b: dict[str, analyzer.TotalsData],
    account_now: pd.DataFrame,
    account_prev: pd.DataFrame,
    use_grouping: bool = False,  # 追加（デフォルト値で既存コードとの互換性を維持）
    grouping_rules: list = None,  # 追加（デフォルト値で既存コードとの互換性を維持）
):
    """サマリーシートを出力"""
    workbook = writer.book
    worksheet = workbook.add_worksheet(sheet_name)
    writer.sheets[sheet_name] = worksheet
    
    # フォーマット設定
    percent_fmt = workbook.add_format({"num_format": "0.0%"})
    number_fmt = workbook.add_format({"num_format": "#,##0"})
    currency_fmt = workbook.add_format({"num_format": "¥#,##0"})
    header_fmt = workbook.add_format({"bold": True, "bg_color": "#D3D3D3"})
    section_header_fmt = workbook.add_format({"bold": True, "bg_color": "#E6E6FA", "font_size": 12})
    
    current_row = 0
    
    # 1. 上部情報（アカウント名、期間、作成日時）
    worksheet.write(current_row, 0, "アカウント名", header_fmt)
    worksheet.write(current_row, 1, account_name)
    current_row += 1
    
    worksheet.write(current_row, 0, "後期間", header_fmt)
    worksheet.write(current_row, 1, period_a_str)
    current_row += 1
    
    worksheet.write(current_row, 0, "前期間", header_fmt)
    worksheet.write(current_row, 1, period_b_str)
    current_row += 1
    
    from datetime import datetime
    worksheet.write(current_row, 0, "作成日時", header_fmt)
    worksheet.write(current_row, 1, datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
    current_row += 1
    
    # キャンペーン合算機能の情報を追加（合算機能を使っている場合のみ表示）
    if use_grouping and grouping_rules:
        worksheet.write(current_row, 0, "キャンペーン合算", header_fmt)
        grouping_info = []
        for rule in grouping_rules:
            rule_name = rule.get("name", "")
            keywords = rule.get("include", [])
            if keywords:
                grouping_info.append(f"{rule_name}: {', '.join(keywords)}")
        worksheet.write(current_row, 1, "使用 (" + "; ".join(grouping_info) + ")")
        current_row += 1
    current_row += 1  # 空行を追加
    
    # 2. アカウント全体の指標
    worksheet.write(current_row, 0, "アカウント全体の指標", section_header_fmt)
    current_row += 1
    
    # 基本指標と効率指標、変化率を計算・表示
    # 変化率の計算関数
    def calc_change_rate(a, b):
        if b == 0:
            return 0.0  # 分母が0の場合は0.0を返す（xlsxwriterがINFをサポートしないため）
        try:
            result = (a - b) / b
            # NANやINFの場合は0.0を返す
            import math
            if not math.isfinite(result):
                return 0.0
        except (ValueError, OverflowError, TypeError, ZeroDivisionError):
            return 0.0
        # NANやINFの場合は0.0を返す
        import math
        if not math.isfinite(result):
            return 0.0
        return result
    
    # 効率指標の計算
    ctr_a = totals_a.click / totals_a.imp if totals_a.imp > 0 else 0
    cvr_a = totals_a.cv / totals_a.click if totals_a.click > 0 else 0
    cpc_a = totals_a.cost / totals_a.click if totals_a.click > 0 else 0
    cpa_a = totals_a.cost / totals_a.cv if totals_a.cv > 0 else 0
    
    ctr_b = totals_b.click / totals_b.imp if totals_b.imp > 0 else 0
    cvr_b = totals_b.cv / totals_b.click if totals_b.click > 0 else 0
    cpc_b = totals_b.cost / totals_b.click if totals_b.click > 0 else 0
    cpa_b = totals_b.cost / totals_b.cv if totals_b.cv > 0 else 0
    
    account_summary_data = {
        "指標": [
            "表示回数", "クリック数", "費用", "応募数",
            "CTR", "CVR", "CPC", "CPA",
        ],
        period_a_str: [
            totals_a.imp, totals_a.click, totals_a.cost, totals_a.cv,
            ctr_a, cvr_a, cpc_a, cpa_a,
        ],
        period_b_str: [
            totals_b.imp, totals_b.click, totals_b.cost, totals_b.cv,
            ctr_b, cvr_b, cpc_b, cpa_b,
        ],
        "増減額": [
            totals_a.imp - totals_b.imp,
            totals_a.click - totals_b.click,
            totals_a.cost - totals_b.cost,
            totals_a.cv - totals_b.cv,
            ctr_a - ctr_b,
            cvr_a - cvr_b,
            cpc_a - cpc_b,
            cpa_a - cpa_b,
        ],
        "増減率": [
            calc_change_rate(totals_a.imp, totals_b.imp),
            calc_change_rate(totals_a.click, totals_b.click),
            calc_change_rate(totals_a.cost, totals_b.cost),
            calc_change_rate(totals_a.cv, totals_b.cv),
            calc_change_rate(ctr_a, ctr_b),
            calc_change_rate(cvr_a, cvr_b),
            calc_change_rate(cpc_a, cpc_b),
            calc_change_rate(cpa_a, cpa_b),
        ],
    }
    
    # ヘッダー行を書き込み
    headers = ["指標", period_a_str, period_b_str, "増減額", "増減率"]
    for col_idx, header in enumerate(headers):
        worksheet.write(current_row, col_idx, header, header_fmt)
    
    current_row += 1
    
    # データ行を書き込み
    metrics = ["表示回数", "クリック数", "費用", "応募数", "CTR", "CVR", "CPC", "CPA"]
    values_a = [totals_a.imp, totals_a.click, totals_a.cost, totals_a.cv, ctr_a, cvr_a, cpc_a, cpa_a]
    values_b = [totals_b.imp, totals_b.click, totals_b.cost, totals_b.cv, ctr_b, cvr_b, cpc_b, cpa_b]
    deltas = [
        totals_a.imp - totals_b.imp,
        totals_a.click - totals_b.click,
        totals_a.cost - totals_b.cost,
        totals_a.cv - totals_b.cv,
        ctr_a - ctr_b,
        cvr_a - cvr_b,
        cpc_a - cpc_b,
        cpa_a - cpa_b,
    ]
    change_rates = [
        calc_change_rate(totals_a.imp, totals_b.imp),
        calc_change_rate(totals_a.click, totals_b.click),
        calc_change_rate(totals_a.cost, totals_b.cost),
        calc_change_rate(totals_a.cv, totals_b.cv),
        calc_change_rate(ctr_a, ctr_b),
        calc_change_rate(cvr_a, cvr_b),
        calc_change_rate(cpc_a, cpc_b),
        calc_change_rate(cpa_a, cpa_b),
    ]
    
    for row_idx, metric in enumerate(metrics):
        # 指標列
        worksheet.write(current_row, 0, metric)
        
        # 期間A列
        if row_idx < 4:  # 基本指標
            if row_idx == 2:  # 費用
                worksheet.write(current_row, 1, values_a[row_idx], currency_fmt)
            else:
                worksheet.write(current_row, 1, values_a[row_idx], number_fmt)
        else:  # 効率指標
            if row_idx == 7:  # CPA
                worksheet.write(current_row, 1, values_a[row_idx], currency_fmt)
            else:
                worksheet.write(current_row, 1, values_a[row_idx], percent_fmt)
        
        # 期間B列
        if row_idx < 4:  # 基本指標
            if row_idx == 2:  # 費用
                worksheet.write(current_row, 2, values_b[row_idx], currency_fmt)
            else:
                worksheet.write(current_row, 2, values_b[row_idx], number_fmt)
        else:  # 効率指標
            if row_idx == 7:  # CPA
                worksheet.write(current_row, 2, values_b[row_idx], currency_fmt)
            else:
                worksheet.write(current_row, 2, values_b[row_idx], percent_fmt)
        
        # 増減額列
        if row_idx < 4:  # 基本指標の増減額
            if row_idx == 2:  # 費用
                worksheet.write(current_row, 3, deltas[row_idx], currency_fmt)
            else:
                worksheet.write(current_row, 3, deltas[row_idx], number_fmt)
        else:  # 効率指標の増減額
            if row_idx == 7:  # CPA
                worksheet.write(current_row, 3, deltas[row_idx], currency_fmt)
            else:
                worksheet.write(current_row, 3, deltas[row_idx], percent_fmt)
        
        # 増減率列（すべて%）
        worksheet.write(current_row, 4, change_rates[row_idx], percent_fmt)
        
        current_row += 1
    
    # 列幅設定
    worksheet.set_column(0, 0, 15)  # 指標列
    worksheet.set_column(1, 1, 15)  # 期間A列
    worksheet.set_column(2, 2, 15)  # 期間B列
    worksheet.set_column(3, 3, 15)  # 増減額列
    worksheet.set_column(4, 4, 12)  # 増減率列
    
    current_row += 2
    
    # 3. キャンペーン別の指標
    worksheet.write(current_row, 0, "キャンペーン別の指標", section_header_fmt)
    current_row += 1

    def safe_divide_single(numerator, denominator):
        return numerator / denominator if denominator != 0 else 0.0

    # 全キャンペーンを取得（どちらかの期間に存在すれば含める）
    all_campaigns = sorted(set(campaign_totals_a.keys()) | set(campaign_totals_b.keys()))
    
    campaign_summary_rows = []
    for campaign in all_campaigns:
        ca = campaign_totals_a.get(campaign, analyzer.TotalsData(imp=0.0, click=0.0, cost=0.0, cv=0.0))
        cb = campaign_totals_b.get(campaign, analyzer.TotalsData(imp=0.0, click=0.0, cost=0.0, cv=0.0))
        
        # 両期間とも実績0（imp/click/cost/cvが全て0）の場合は除外
        if (
            ca.imp == 0 and ca.click == 0 and ca.cost == 0 and ca.cv == 0
            and cb.imp == 0 and cb.click == 0 and cb.cost == 0 and cb.cv == 0
        ):
            continue

        # 費用シェア（アカウント全体に対する）
        cost_share_a = safe_divide_single(ca.cost, totals_a.cost)
        cost_share_b = safe_divide_single(cb.cost, totals_b.cost)

        # 効率指標
        ctr_a = safe_divide_single(ca.click, ca.imp)
        ctr_b = safe_divide_single(cb.click, cb.imp)
        cvr_a = safe_divide_single(ca.cv, ca.click)
        cvr_b = safe_divide_single(cb.cv, cb.click)
        cpc_a = safe_divide_single(ca.cost, ca.click)
        cpc_b = safe_divide_single(cb.cost, cb.click)
        cpa_a = safe_divide_single(ca.cost, ca.cv)
        cpa_b = safe_divide_single(cb.cost, cb.cv)

        campaign_summary_rows.append({
            "キャンペーン名": campaign,

            # ---- 後期間の実績 ----
            f"{period_a_str}_費用シェア": cost_share_a,
            "費用シェア_差分": cost_share_a - cost_share_b,

            f"{period_a_str}_表示回数": ca.imp,
            f"{period_a_str}_クリック数": ca.click,
            f"{period_a_str}_費用": ca.cost,
            f"{period_a_str}_応募数": ca.cv,

            f"{period_a_str}_CTR": ctr_a,
            f"{period_a_str}_CVR": cvr_a,
            f"{period_a_str}_CPC": cpc_a,
            f"{period_a_str}_CPA": cpa_a,

            # ---- 前期間からの変化率 ----
            "表示回数_増減率": calc_change_rate(ca.imp, cb.imp),
            "クリック数_増減率": calc_change_rate(ca.click, cb.click),
            "費用_増減率": calc_change_rate(ca.cost, cb.cost),
            "応募数_増減率": calc_change_rate(ca.cv, cb.cv),
            "CTR_増減率": calc_change_rate(ctr_a, ctr_b),
            "CVR_増減率": calc_change_rate(cvr_a, cvr_b),
            "CPC_増減率": calc_change_rate(cpc_a, cpc_b),
            "CPA_増減率": calc_change_rate(cpa_a, cpa_b),
        })
    
    campaign_summary_df = pd.DataFrame(campaign_summary_rows)
    
    # コストシェアが大きい順にソート（後期間のコストシェアで）
    if len(campaign_summary_df) > 0:
        cost_share_col = f"{period_a_str}_費用シェア"
        campaign_summary_df = campaign_summary_df.sort_values(cost_share_col, ascending=False)
    
    # ヘッダー行とデータ行を書き込み
    if len(campaign_summary_df) > 0:
        headers = list(campaign_summary_df.columns)
        
        # ヘッダー行を書き込み
        for col_idx, header in enumerate(headers):
            worksheet.write(current_row, col_idx, header, header_fmt)
        current_row += 1
        
        # データ行を書き込み
        for _, row in campaign_summary_df.iterrows():
            for col_idx, col_name in enumerate(headers):
                value = row[col_name]
                
                # フォーマットを決定
                if col_name == "キャンペーン名":
                    fmt = None
                elif "シェア" in col_name or "増減率" in col_name or "CTR" in col_name or "CVR" in col_name:
                    fmt = percent_fmt
                elif "費用" in col_name or "CPC" in col_name or "CPA" in col_name:
                    fmt = currency_fmt
                else:
                    fmt = number_fmt
                
                if fmt:
                    worksheet.write(current_row, col_idx, value, fmt)
                else:
                    worksheet.write(current_row, col_idx, value)
            current_row += 1
        
        # 列幅設定（横に広くなるので、基本は一律で確保）
        worksheet.set_column(0, 0, 50)  # キャンペーン名列
        if len(headers) > 1:
            worksheet.set_column(1, len(headers) - 1, 14)
    
    current_row += 2
    
    # 4. 影響が大きいキーワード
    worksheet.write(current_row, 0, "影響が大きいキーワード", section_header_fmt)
    current_row += 1
    
    # キーワードの増減額を計算
    account_now_reset = account_now.reset_index()
    account_prev_reset = account_prev.reset_index()
    
    # マージして増減額を計算
    keyword_delta = account_now_reset.merge(
        account_prev_reset[["keyword", "imp", "click", "cost", "cv"]],
        on="keyword",
        suffixes=("_a", "_b")
    )
    keyword_delta["imp_delta"] = keyword_delta["imp_a"] - keyword_delta["imp_b"]
    keyword_delta["click_delta"] = keyword_delta["click_a"] - keyword_delta["click_b"]
    keyword_delta["cost_delta"] = keyword_delta["cost_a"] - keyword_delta["cost_b"]
    keyword_delta["cv_delta"] = keyword_delta["cv_a"] - keyword_delta["cv_b"]
    
    # シェアとシェアの差分を計算
    def safe_divide_single(numerator, denominator):
        return numerator / denominator if denominator != 0 else 0.0
    
    keyword_delta["imp_share_a"] = keyword_delta["imp_a"].apply(lambda x: safe_divide_single(x, totals_a.imp))
    keyword_delta["imp_share_b"] = keyword_delta["imp_b"].apply(lambda x: safe_divide_single(x, totals_b.imp))
    keyword_delta["imp_share_diff"] = keyword_delta["imp_share_a"] - keyword_delta["imp_share_b"]
    
    keyword_delta["click_share_a"] = keyword_delta["click_a"].apply(lambda x: safe_divide_single(x, totals_a.click))
    keyword_delta["click_share_b"] = keyword_delta["click_b"].apply(lambda x: safe_divide_single(x, totals_b.click))
    keyword_delta["click_share_diff"] = keyword_delta["click_share_a"] - keyword_delta["click_share_b"]
    
    keyword_delta["cost_share_a"] = keyword_delta["cost_a"].apply(lambda x: safe_divide_single(x, totals_a.cost))
    keyword_delta["cost_share_b"] = keyword_delta["cost_b"].apply(lambda x: safe_divide_single(x, totals_b.cost))
    keyword_delta["cost_share_diff"] = keyword_delta["cost_share_a"] - keyword_delta["cost_share_b"]
    
    keyword_delta["cv_share_a"] = keyword_delta["cv_a"].apply(lambda x: safe_divide_single(x, totals_a.cv))
    keyword_delta["cv_share_b"] = keyword_delta["cv_b"].apply(lambda x: safe_divide_single(x, totals_b.cv))
    keyword_delta["cv_share_diff"] = keyword_delta["cv_share_a"] - keyword_delta["cv_share_b"]
    
    # 各指標ごとにTOP5（増加・減少）を抽出
    metrics = [
        ("費用", "cost", "cost_delta", "cost_share_diff"),
        ("応募数", "cv", "cv_delta", "cv_share_diff"),
        ("表示回数", "imp", "imp_delta", "imp_share_diff"),
        ("クリック数", "click", "click_delta", "click_share_diff"),
    ]
    
    for metric_name, metric_col, delta_col, share_diff_col in metrics:
        worksheet.write(current_row, 0, f"{metric_name}の増減", section_header_fmt)
        current_row += 1
        
        # 1. 増減額TOP5
        worksheet.write(current_row, 0, f"{metric_name}増減額TOP5", header_fmt)
        current_row += 1
        
        # 増加TOP5（増減額でソート）
        top5_increase_delta = keyword_delta.nlargest(5, delta_col).copy()
        top5_increase_delta["増減率"] = top5_increase_delta.apply(
            lambda row: calc_change_rate(row[f"{metric_col}_a"], row[f"{metric_col}_b"]), axis=1
        )
        top5_increase_delta = top5_increase_delta[["keyword", delta_col, "増減率", f"{metric_col}_a", f"{metric_col}_b"]]
        top5_increase_delta.columns = ["キーワード", "増減額", "増減率", period_a_str, period_b_str]
        
        # 減少TOP5（増減額でソート）
        top5_decrease_delta = keyword_delta.nsmallest(5, delta_col).copy()
        top5_decrease_delta["増減率"] = top5_decrease_delta.apply(
            lambda row: calc_change_rate(row[f"{metric_col}_a"], row[f"{metric_col}_b"]), axis=1
        )
        top5_decrease_delta = top5_decrease_delta[["keyword", delta_col, "増減率", f"{metric_col}_a", f"{metric_col}_b"]]
        top5_decrease_delta.columns = ["キーワード", "増減額", "増減率", period_a_str, period_b_str]
        
        # 横並びで表示（増減額TOP5）
        worksheet.write(current_row, 0, f"{metric_name}増加TOP5", header_fmt)
        worksheet.write(current_row, 5, f"{metric_name}減少TOP5", header_fmt)
        current_row += 1
        
        # ヘッダー（増減額TOP5）
        headers_delta = ["キーワード", "増減額", "増減率", period_a_str, period_b_str]
        for i, col in enumerate(headers_delta):
            worksheet.write(current_row, i, col, header_fmt)
        for i, col in enumerate(headers_delta):
            worksheet.write(current_row, i + 5, col, header_fmt)
        current_row += 1
        
        # データ（増減額TOP5）
        max_rows_delta = max(len(top5_increase_delta), len(top5_decrease_delta))
        for i in range(max_rows_delta):
            if i < len(top5_increase_delta):
                row = top5_increase_delta.iloc[i]
                worksheet.write(current_row, 0, row["キーワード"])
                worksheet.write(current_row, 1, row["増減額"], number_fmt if metric_col != "cost" else currency_fmt)
                # 増減率は% - INF/NANを0.0に変換
                change_rate = row["増減率"]
                if not isinstance(change_rate, (int, float)) or (isinstance(change_rate, float) and (change_rate != change_rate or abs(change_rate) == float('inf'))):
                    change_rate = 0.0
                worksheet.write(current_row, 2, change_rate, percent_fmt)
                # period_a_strとperiod_b_strがSeriesを返す可能性があるため、スカラー値に変換
                value_a = row[period_a_str]
                if isinstance(value_a, pd.Series):
                    value_a = value_a.iloc[0] if len(value_a) > 0 else 0
                value_b = row[period_b_str]
                if isinstance(value_b, pd.Series):
                    value_b = value_b.iloc[0] if len(value_b) > 0 else 0
                worksheet.write(current_row, 3, value_a, number_fmt if metric_col != "cost" else currency_fmt)
                worksheet.write(current_row, 4, value_b, number_fmt if metric_col != "cost" else currency_fmt)
            if i < len(top5_decrease_delta):
                row = top5_decrease_delta.iloc[i]
                worksheet.write(current_row, 5, row["キーワード"])
                worksheet.write(current_row, 6, row["増減額"], number_fmt if metric_col != "cost" else currency_fmt)
                # 増減率は% - INF/NANを0.0に変換
                change_rate = row["増減率"]
                if not isinstance(change_rate, (int, float)) or (isinstance(change_rate, float) and (change_rate != change_rate or abs(change_rate) == float('inf'))):
                    change_rate = 0.0
                worksheet.write(current_row, 7, change_rate, percent_fmt)
                # period_a_strとperiod_b_strがSeriesを返す可能性があるため、スカラー値に変換
                value_a = row[period_a_str]
                if isinstance(value_a, pd.Series):
                    value_a = value_a.iloc[0] if len(value_a) > 0 else 0
                value_b = row[period_b_str]
                if isinstance(value_b, pd.Series):
                    value_b = value_b.iloc[0] if len(value_b) > 0 else 0
                worksheet.write(current_row, 8, value_a, number_fmt if metric_col != "cost" else currency_fmt)
                worksheet.write(current_row, 9, value_b, number_fmt if metric_col != "cost" else currency_fmt)
            current_row += 1
        
        # 列幅設定（増減額TOP5）
        worksheet.set_column(0, 0, 30)  # キーワード列
        worksheet.set_column(1, 1, 15, number_fmt if metric_col != "cost" else currency_fmt)  # 増減額
        worksheet.set_column(2, 2, 15, percent_fmt)  # 増減率（%）
        worksheet.set_column(3, 3, 15, number_fmt if metric_col != "cost" else currency_fmt)  # 後期間
        worksheet.set_column(4, 4, 15, number_fmt if metric_col != "cost" else currency_fmt)  # 前期間
        worksheet.set_column(5, 5, 30)  # キーワード列
        worksheet.set_column(6, 6, 15, number_fmt if metric_col != "cost" else currency_fmt)  # 増減額
        worksheet.set_column(7, 7, 15, percent_fmt)  # 増減率（%）
        worksheet.set_column(8, 8, 15, number_fmt if metric_col != "cost" else currency_fmt)  # 後期間
        worksheet.set_column(9, 9, 15, number_fmt if metric_col != "cost" else currency_fmt)  # 前期間
        
        current_row += 2
        
        # 2. シェアの差分TOP5
        # セクションタイトルを削除
        
        # 増加TOP5（シェアの差分でソート）
        top5_increase_share = keyword_delta.nlargest(5, share_diff_col).copy()
        # シェア変化率を計算（前期間に対する変化率）
        share_a_col = f"{metric_col}_share_a"
        share_b_col = f"{metric_col}_share_b"
        top5_increase_share["シェア変化率"] = top5_increase_share.apply(
            lambda row: calc_change_rate(row[share_a_col], row[share_b_col]), axis=1
        )
        top5_increase_share = top5_increase_share[["keyword", share_diff_col, "シェア変化率", share_a_col, share_b_col]]
        top5_increase_share.columns = ["キーワード", "シェア変化差分", "シェア変化率", f"シェア（{period_a_str}）", f"シェア（{period_b_str}）"]
        
        # 減少TOP5（シェアの差分でソート）
        top5_decrease_share = keyword_delta.nsmallest(5, share_diff_col).copy()
        # シェア変化率を計算（前期間に対する変化率）
        top5_decrease_share["シェア変化率"] = top5_decrease_share.apply(
            lambda row: calc_change_rate(row[share_a_col], row[share_b_col]), axis=1
        )
        top5_decrease_share = top5_decrease_share[["keyword", share_diff_col, "シェア変化率", share_a_col, share_b_col]]
        top5_decrease_share.columns = ["キーワード", "シェア変化差分", "シェア変化率", f"シェア（{period_a_str}）", f"シェア（{period_b_str}）"]
        
        # 横並びで表示（シェアの差分TOP5）
        worksheet.write(current_row, 0, f"{metric_name}シェア拡大TOP5", header_fmt)
        worksheet.write(current_row, 5, f"{metric_name}シェア縮小TOP5", header_fmt)
        current_row += 1
        
        # ヘッダー（シェアの差分TOP5）
        headers_share = ["キーワード", "シェア変化差分", "シェア変化率", f"シェア（{period_a_str}）", f"シェア（{period_b_str}）"]
        for i, col in enumerate(headers_share):
            worksheet.write(current_row, i, col, header_fmt)
        for i, col in enumerate(headers_share):
            worksheet.write(current_row, i + 5, col, header_fmt)
        current_row += 1
        
        # データ（シェアの差分TOP5）
        max_rows_share = max(len(top5_increase_share), len(top5_decrease_share))
        for i in range(max_rows_share):
            if i < len(top5_increase_share):
                row = top5_increase_share.iloc[i]
                worksheet.write(current_row, 0, row["キーワード"])
                worksheet.write(current_row, 1, row["シェア変化差分"], percent_fmt)  # シェア変化差分は%
                # シェア変化率は% - INF/NANを0.0に変換
                change_rate = row["シェア変化率"]
                if not isinstance(change_rate, (int, float)) or (isinstance(change_rate, float) and (change_rate != change_rate or abs(change_rate) == float('inf'))):
                    change_rate = 0.0
                worksheet.write(current_row, 2, change_rate, percent_fmt)
                worksheet.write(current_row, 3, row[f"シェア（{period_a_str}）"], percent_fmt)  # シェア（後期間）は%
                worksheet.write(current_row, 4, row[f"シェア（{period_b_str}）"], percent_fmt)  # シェア（前期間）は%
            if i < len(top5_decrease_share):
                row = top5_decrease_share.iloc[i]
                worksheet.write(current_row, 5, row["キーワード"])
                worksheet.write(current_row, 6, row["シェア変化差分"], percent_fmt)  # シェア変化差分は%
                # シェア変化率は% - INF/NANを0.0に変換
                change_rate = row["シェア変化率"]
                if not isinstance(change_rate, (int, float)) or (isinstance(change_rate, float) and (change_rate != change_rate or abs(change_rate) == float('inf'))):
                    change_rate = 0.0
                worksheet.write(current_row, 7, change_rate, percent_fmt)
                worksheet.write(current_row, 8, row[f"シェア（{period_a_str}）"], percent_fmt)  # シェア（後期間）は%
                worksheet.write(current_row, 9, row[f"シェア（{period_b_str}）"], percent_fmt)  # シェア（前期間）は%
            current_row += 1
        
        # 列幅設定（シェアの差分TOP5）
        worksheet.set_column(0, 0, 30)  # キーワード列
        worksheet.set_column(1, 1, 15, percent_fmt)  # シェア変化差分（%）
        worksheet.set_column(2, 2, 15, percent_fmt)  # シェア変化率（%）
        worksheet.set_column(3, 3, 15, percent_fmt)  # シェア（後期間）（%）
        worksheet.set_column(4, 4, 15, percent_fmt)  # シェア（前期間）（%）
        worksheet.set_column(5, 5, 30)  # キーワード列
        worksheet.set_column(6, 6, 15, percent_fmt)  # シェア変化差分（%）
        worksheet.set_column(7, 7, 15, percent_fmt)  # シェア変化率（%）
        worksheet.set_column(8, 8, 15, percent_fmt)  # シェア（後期間）（%）
        worksheet.set_column(9, 9, 15, percent_fmt)  # シェア（前期間）（%）
        
        current_row += 2


# =====================
# Main
# =====================

def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------
    # Guard: campaign grouping is NOT supported in run.py
    #
    # キャンペーン合算（config.USE_CAMPAIGN_GROUP=True）は run.py では
    # - シートが二重生成され `_2` が付く
    # - Campaigns.csv totals との突合が崩れて CV/平均CPA が不整合になりやすい
    # といった事故が起きやすい。
    #
    # 合算したい場合は run_grouping.py / アカウント分析くん を使用する。
    # ------------------------------------------------------------
    if getattr(config, "USE_CAMPAIGN_GROUP", False):
        raise RuntimeError(
            "キャンペーン合算（USE_CAMPAIGN_GROUP=True）は run.py では実行できません。\n"
            "合算を行う場合は `python run_grouping.py` もしくは `アカウント分析くん.command` を使用してください。"
        )

    # ---------- load ----------
    period_a_files, period_b_files = analyzer.load_period_files(args.input_dir)
    df_a = analyzer.load_period(period_a_files)
    df_b = analyzer.load_period(period_b_files)

    period_a = analyzer.build_period_data(df_a)
    period_b = analyzer.build_period_data(df_b)

    # ---------- totals (外部ファイルから読み込み) ----------
    totals_dir = args.input_dir / "totals"
    campaign_totals_a, campaign_totals_b, account_name, period_a_str, period_b_str, period_a_full_str, period_b_full_str = analyzer.load_totals_files(totals_dir)
    
    # アカウント名と期間を表示
    print(f"アカウント名: {account_name}")
    print(f"後期間: {period_a_str}")
    print(f"前期間: {period_b_str}")
    
    # アカウント単位の合計値はキャンペーン単位から合算
    totals_a = analyzer.calculate_account_totals(campaign_totals_a)
    totals_b = analyzer.calculate_account_totals(campaign_totals_b)

    # ---------- account ----------
    account_now = analyzer.add_share(
        period_a.account_keyword,
        totals_a,
    ).set_index("keyword")
    
    account_prev = analyzer.add_share(
        period_b.account_keyword,
        totals_b,
    ).set_index("keyword")

    account_delta = analyzer.add_share_delta(
        period_a.account_keyword,
        period_b.account_keyword,
        totals_a,
        totals_b,
        keys=["keyword"],
    ).set_index("keyword")

    # ---------- campaign ----------
    campaign_sheets = {}

    for campaign in period_a.campaign_keyword["campaign_name"].unique():
        a = period_a.campaign_keyword.query("campaign_name == @campaign")
        b = period_b.campaign_keyword.query("campaign_name == @campaign")

        if a.empty or b.empty:
            continue

        # 外部ファイルからキャンペーン単位の合計値を取得
        # 合算が有効な場合、合算前のキャンペーン名で取得する必要がある
        if config.USE_CAMPAIGN_GROUP:
            # 合算前のキャンペーン名を取得
            original_campaigns = period_a.raw[
                period_a.raw["campaign_name"] == campaign
            ]["campaign_name"].unique().tolist()
            
            # 合算前のキャンペーン名の合計値を集計
            totals_a_c = analyzer.get_campaign_totals_for_grouped_campaign(
                campaign_totals_a,
                original_campaigns,
            )
            totals_b_c = analyzer.get_campaign_totals_for_grouped_campaign(
                campaign_totals_b,
                original_campaigns,
            )
        else:
            # 合算が無効な場合、キャンペーン名で直接突合
            if campaign in campaign_totals_a and campaign in campaign_totals_b:
                # 外部ファイルから取得
                totals_a_c = campaign_totals_a[campaign]
                totals_b_c = campaign_totals_b[campaign]
            else:
                # 外部ファイルにない場合、キーワードデータから合計値を計算
                totals_a_c = analyzer.TotalsData(
                    imp=a["imp"].sum(),
                    click=a["click"].sum(),
                    cost=a["cost"].sum(),
                    cv=a["cv"].sum(),
                )
                totals_b_c = analyzer.TotalsData(
                    imp=b["imp"].sum(),
                    click=b["click"].sum(),
                    cost=b["cost"].sum(),
                    cv=b["cv"].sum(),
                )

        now = analyzer.add_share(a, totals_a_c).set_index("keyword")
        prev = analyzer.add_share(b, totals_b_c).set_index("keyword")
        delta = analyzer.add_share_delta(
            a,
            b,
            totals_a_c,
            totals_b_c,
            keys=["campaign_name", "keyword"],
        ).set_index("keyword")

        campaign_sheets[campaign] = (now, prev, delta, totals_a_c, totals_b_c)

    # ---------- Excel ----------
    safe_account_name = _sanitize_filename_component(account_name)
    output_filename = f"{safe_account_name}_KWレポート_{period_b_full_str}__{period_a_full_str}.xlsx"
    output_path = _make_unique_path(args.output_dir / output_filename)
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        # サマリーシートを最初に追加
        write_summary_sheet(
            writer,
            "サマリー",
            account_name,
            period_a_str,
            period_b_str,
            totals_a,
            totals_b,
            campaign_totals_a,
            campaign_totals_b,
            account_now,
            account_prev,
        )
        
        # アカウント: 今期と前期を別シートに
        account_avg_cpa_a = totals_a.cost / totals_a.cv if totals_a.cv > 0 else 0
        account_avg_cpa_b = totals_b.cost / totals_b.cv if totals_b.cv > 0 else 0
        grouping_rules_for_sheet = config.CAMPAIGN_GROUP_RULES if config.USE_CAMPAIGN_GROUP else None
        write_sheet_now_only(writer, f"アカウント_{period_a_str}", account_now, account_avg_cpa_a, grouping_rules_for_sheet)
        write_sheet_now_only(writer, f"アカウント_{period_b_str}", account_prev, account_avg_cpa_b, grouping_rules_for_sheet)
        
        # キャンペーン: 今期と前期を別シートに
        for campaign, (now_df, prev_df, delta_df, totals_a_c, totals_b_c) in campaign_sheets.items():
            # Excelのシート名に使用できない文字を除去
            invalid_chars = ['[', ']', ':', '*', '?', '/', '\\']
            base_name = campaign
            for char in invalid_chars:
                base_name = base_name.replace(char, '_')
            
            # 期間文字列（例：0101-0131）は9文字、Excelのシート名は31文字制限
            # シート名の形式: "{base_name}_{period_str}" なので、base_nameは最大22文字
            period_suffix_len = len(f"_{period_a_str}")  # アンダースコア1文字 + 期間文字列9文字 = 10文字
            max_base_len = 31 - period_suffix_len  # 31 - 10 = 21文字まで
            if len(base_name) > max_base_len:
                base_name = base_name[:max_base_len]
            
            # キャンペーン全体のCPA平均を計算（加重平均）
            campaign_avg_cpa_a = totals_a_c.cost / totals_a_c.cv if totals_a_c.cv > 0 else 0
            campaign_avg_cpa_b = totals_b_c.cost / totals_b_c.cv if totals_b_c.cv > 0 else 0
            
            write_sheet_now_only(writer, f"{base_name}_{period_a_str}", now_df, campaign_avg_cpa_a, grouping_rules_for_sheet)
            write_sheet_now_only(writer, f"{base_name}_{period_b_str}", prev_df, campaign_avg_cpa_b, grouping_rules_for_sheet)
        
        # キャンペーン合算シート（USE_CAMPAIGN_GROUPがTrueの場合）
        if config.USE_CAMPAIGN_GROUP:
            # 合算前のデータから合算グループを構築
            # period_a.rawには合算前のデータが含まれている
            original_campaigns_a = set(df_a["campaign_name"].unique())
            original_campaigns_b = set(df_b["campaign_name"].unique())
            
            # 合算ルールに基づいて、どの元キャンペーンがどの合算グループに属するかを判定
            grouped_campaigns_map = {}  # {合算後の名前: [元のキャンペーン名のリスト]}
            
            for original_campaign in original_campaigns_a:
                # 合算後のキャンペーン名を取得
                test_df = pd.DataFrame({"campaign_name": [original_campaign]})
                grouped_df = analyzer.apply_campaign_grouping(test_df)
                grouped_name = grouped_df["campaign_name"].iloc[0]
                
                if grouped_name not in grouped_campaigns_map:
                    grouped_campaigns_map[grouped_name] = []
                grouped_campaigns_map[grouped_name].append(original_campaign)
            
            # 合算シートを作成（合算前のキャンペーン名と異なる場合のみ）
            for grouped_name, original_campaigns in grouped_campaigns_map.items():
                if grouped_name in original_campaigns:
                    continue  # 合算されていないキャンペーンはスキップ
                
                # 合算前のキャンペーン名の合計値を集計
                totals_a_grouped = analyzer.get_campaign_totals_for_grouped_campaign(
                    campaign_totals_a,
                    original_campaigns,
                )
                totals_b_grouped = analyzer.get_campaign_totals_for_grouped_campaign(
                    campaign_totals_b,
                    original_campaigns,
                )
                
                # 合算後のキャンペーンのキーワードデータを取得
                a = period_a.campaign_keyword.query("campaign_name == @grouped_name")
                b = period_b.campaign_keyword.query("campaign_name == @grouped_name")
                
                if a.empty or b.empty:
                    continue
                
                now = analyzer.add_share(a, totals_a_grouped).set_index("keyword")
                prev = analyzer.add_share(b, totals_b_grouped).set_index("keyword")
                
                # シート名を整形
                invalid_chars = ['[', ']', ':', '*', '?', '/', '\\']
                sheet_name = grouped_name
                for char in invalid_chars:
                    sheet_name = sheet_name.replace(char, '_')
                
                # 期間文字列（例：0101-0131）は9文字、Excelのシート名は31文字制限
                # シート名の形式: "{sheet_name}_{period_str}" なので、sheet_nameは最大22文字
                period_suffix_len = len(f"_{period_a_str}")  # アンダースコア1文字 + 期間文字列9文字 = 10文字
                max_sheet_len = 31 - period_suffix_len  # 31 - 10 = 21文字まで
                if len(sheet_name) > max_sheet_len:
                    sheet_name = sheet_name[:max_sheet_len]
                
                # キャンペーン全体のCPA平均を計算（加重平均）
                grouped_avg_cpa_a = totals_a_grouped.cost / totals_a_grouped.cv if totals_a_grouped.cv > 0 else 0
                grouped_avg_cpa_b = totals_b_grouped.cost / totals_b_grouped.cv if totals_b_grouped.cv > 0 else 0
                
                write_sheet_now_only(writer, f"{sheet_name}_{period_a_str}", now, grouped_avg_cpa_a, grouping_rules_for_sheet)
                write_sheet_now_only(writer, f"{sheet_name}_{period_b_str}", prev, grouped_avg_cpa_b, grouping_rules_for_sheet)

    print(f"Excel 出力完了: {output_path}")


if __name__ == "__main__":
    main()
