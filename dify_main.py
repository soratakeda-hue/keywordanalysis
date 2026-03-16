#!/usr/bin/env python3
"""
Dify用キーワード分析統合スクリプト

このスクリプトは、既存のrun.pyとrun_grouping.pyの機能を統合し、
Difyのワークフローで実行できる形式に変換したものです。

入力:
- period_a_excel_path: 後期間のExcelファイルパス
- period_b_excel_path: 前期間のExcelファイルパス
- period_a_csv_path: 後期間のCampaigns.csvファイルパス
- period_b_csv_path: 前期間のCampaigns.csvファイルパス
- grouping_rules_text: 合算ルール（任意、カンマ区切り: "看護師,介護"）

出力:
- Excelファイル（BytesIO）
"""

import io
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Dict, Optional, Iterable

import pandas as pd


# =====================
# Config (config.py相当)
# =====================

COLUMN_MAP = {
    "キーワード": "keyword",
    "キャンペーンID": "campaign_id",
    "キャンペーン名": "campaign_name",
    "表示回数": "imp",
    "クリック数": "click",
    "クリック率": "click_rate",
    "平均CPC": "avg_cpc",
    "消化予算": "cost",
    "応募数": "cv",
    "応募率": "cv_rate",
    "応募単価": "cpa",
    "平均表示順位": "avg_position",
}

REQUIRED_COLUMNS = {
    "keyword",
    "campaign_id",
    "campaign_name",
    "imp",
    "click",
    "cost",
    "cv",
}

NUMERIC_COLUMNS = {
    "imp",
    "click",
    "cost",
    "cv",
    "click_rate",
    "avg_cpc",
    "cv_rate",
    "cpa",
    "avg_position",
}

# グローバル設定（実行時に上書き）
USE_CAMPAIGN_GROUP = False
CAMPAIGN_GROUP_RULES = []


# =====================
# Data classes (analyzer.py相当)
# =====================

@dataclass
class PeriodData:
    raw: pd.DataFrame
    account_keyword: pd.DataFrame
    campaign_keyword: pd.DataFrame
    campaign_totals: pd.DataFrame


@dataclass
class TotalsData:
    imp: float
    click: float
    cost: float
    cv: float


# =====================
# Utilities (analyzer.py相当)
# =====================

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    mapped = {}
    for col in df.columns:
        normalized = str(col).strip().replace("　", " ")
        mapped[col] = COLUMN_MAP.get(normalized, normalized)
    return df.rename(columns=mapped)


def coerce_numeric(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("¥", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.replace("-", "", regex=False)
        .str.strip()
    )
    return pd.to_numeric(cleaned, errors="coerce").fillna(0)


def safe_divide(n: pd.Series, d) -> pd.Series:
    if isinstance(d, pd.Series):
        return (n / d.replace(0, pd.NA)).fillna(0)
    if d == 0:
        return 0
    return n / d


# =====================
# Loaders (analyzer.py相当)
# =====================

def load_file_from_path(path: str) -> pd.DataFrame:
    """ファイルパス（文字列）からDataFrameを読み込む"""
    path_obj = Path(path)
    if path_obj.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(path, engine="openpyxl")
    else:
        df = pd.read_csv(path, encoding="utf-8-sig", thousands=",")

    df = normalize_columns(df)

    missing = sorted(REQUIRED_COLUMNS - set(df.columns))
    if missing:
        raise ValueError(f"{path_obj.name} is missing required columns: {', '.join(missing)}")

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = coerce_numeric(df[col])

    df["keyword"] = df["keyword"].astype(str)
    df["campaign_name"] = df["campaign_name"].astype(str)
    df["campaign_id"] = df["campaign_id"].astype(str)

    return df


def load_campaign_totals_from_csv_path(csv_path: str) -> Dict[str, TotalsData]:
    """CSVファイルパスからキャンペーン単位の合計値を読み込む"""
    df = pd.read_csv(csv_path, encoding="shift_jis")

    column_mapping = {
        "キャンペーン名": "campaign_name",
        "表示回数": "imp",
        "クリック数": "click",
        "消化予算": "cost",
        "応募数": "cv",
    }

    df_renamed = df.rename(columns=column_mapping)

    required_cols = ["campaign_name", "imp", "click", "cost", "cv"]
    missing = [col for col in required_cols if col not in df_renamed.columns]
    if missing:
        raise ValueError(f"CSV is missing required columns: {', '.join(missing)}")

    df_renamed["campaign_name"] = df_renamed["campaign_name"].astype(str).str.strip()

    df_renamed = df_renamed[
        df_renamed["campaign_name"].notna()
        & (df_renamed["campaign_name"] != "")
        & (df_renamed["campaign_name"] != "合計")
    ].copy()

    for col in ["imp", "click", "cost", "cv"]:
        df_renamed[col] = pd.to_numeric(df_renamed[col], errors="coerce").fillna(0).astype(float)

    grouped = (
        df_renamed.groupby("campaign_name", as_index=False)[["imp", "click", "cost", "cv"]]
        .sum()
    )

    result: Dict[str, TotalsData] = {}
    for _, row in grouped.iterrows():
        campaign_name = str(row["campaign_name"]).strip()
        result[campaign_name] = TotalsData(
            imp=float(row["imp"]),
            click=float(row["click"]),
            cost=float(row["cost"]),
            cv=float(row["cv"]),
        )

    return result


def extract_period_from_csv_filename(filename: str) -> Tuple[str, str]:
    """
    CSVファイル名から期間文字列を抽出

    Returns:
        (period_str: "mmdd-mmdd", period_full_str: "YYYYMMDD-YYYYMMDD")
    """
    pattern = r"(\d{8})-(\d{8})"
    match = re.search(pattern, filename)

    if match:
        start_a = datetime.strptime(match.group(1), "%Y%m%d")
        end_a = datetime.strptime(match.group(2), "%Y%m%d")
        period_str = f"{start_a.strftime('%m%d')}-{end_a.strftime('%m%d')}"
        period_full_str = f"{match.group(1)}-{match.group(2)}"
        return period_str, period_full_str
    else:
        return "期間", "期間"


def extract_account_name_from_filename(filename: str) -> str:
    """CSVファイル名からアカウント名を抽出"""
    start_marker = "Report_"
    start_idx = filename.find(start_marker)
    if start_idx == -1:
        return "アカウント"

    start_idx += len(start_marker)
    end_marker = "_すべて"
    end_idx = filename.find(end_marker, start_idx)

    if end_idx == -1 or end_idx <= start_idx:
        return "アカウント"

    account_name = filename[start_idx:end_idx]
    return account_name


# =====================
# KPI calculations (analyzer.py相当)
# =====================

def add_kpis(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ctr"] = safe_divide(df["click"], df["imp"])
    df["cvr"] = safe_divide(df["cv"], df["click"])
    df["cpc"] = safe_divide(df["cost"], df["click"])
    df["cpa"] = safe_divide(df["cost"], df["cv"])
    return df


def add_share(df: pd.DataFrame, totals: TotalsData) -> pd.DataFrame:
    df = df.copy()
    df["imp_share"] = safe_divide(df["imp"], totals.imp)
    df["click_share"] = safe_divide(df["click"], totals.click)
    df["cost_share"] = safe_divide(df["cost"], totals.cost)
    df["cv_share"] = safe_divide(df["cv"], totals.cv)
    return df


def add_share_delta(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    totals_a: TotalsData,
    totals_b: TotalsData,
    keys: List[str],
) -> pd.DataFrame:
    merged = df_a.merge(
        df_b,
        on=keys,
        how="outer",
        suffixes=("_A", "_B"),
    ).fillna(0)

    for m in ["imp", "click", "cost", "cv"]:
        share_a = safe_divide(merged[f"{m}_A"], getattr(totals_a, m))
        share_b = safe_divide(merged[f"{m}_B"], getattr(totals_b, m))
        merged[f"delta_{m}_share"] = share_a - share_b

    return merged


# =====================
# Campaign grouping (analyzer.py相当)
# =====================

def apply_campaign_grouping(df: pd.DataFrame) -> pd.DataFrame:
    if not USE_CAMPAIGN_GROUP:
        return df

    df = df.copy()

    def map_campaign(name: str) -> str:
        for rule in CAMPAIGN_GROUP_RULES:
            include = rule.get("include", [])
            exclude = rule.get("exclude", [])

            if exclude and any(k in name for k in exclude):
                continue

            if include:
                for keyword in include:
                    if keyword in name:
                        return keyword

        return name

    df["campaign_name"] = df["campaign_name"].map(map_campaign)
    return df


def apply_grouping_to_campaign_totals(
    campaign_totals: Dict[str, TotalsData],
    grouping_rules: List[Dict],
) -> Dict[str, TotalsData]:
    """キャンペーン合計データを合算後の名前で再集計"""
    if not grouping_rules:
        return campaign_totals

    def map_campaign(name: str) -> str:
        for rule in grouping_rules:
            include = rule.get("include", [])
            exclude = rule.get("exclude", [])

            if exclude and any(k in name for k in exclude):
                continue

            if include:
                for keyword in include:
                    if keyword in name:
                        return keyword

        return name

    grouped_totals = {}
    for campaign_name, totals in campaign_totals.items():
        grouped_name = map_campaign(campaign_name)

        if grouped_name not in grouped_totals:
            grouped_totals[grouped_name] = TotalsData(
                imp=0.0,
                click=0.0,
                cost=0.0,
                cv=0.0,
            )

        grouped_totals[grouped_name].imp += totals.imp
        grouped_totals[grouped_name].click += totals.click
        grouped_totals[grouped_name].cost += totals.cost
        grouped_totals[grouped_name].cv += totals.cv

    return grouped_totals


# =====================
# Period builder (analyzer.py相当)
# =====================

def build_period_data(df: pd.DataFrame) -> PeriodData:
    df = apply_campaign_grouping(df)

    metrics = ["imp", "click", "cost", "cv"]

    account = df.groupby("keyword", as_index=False)[metrics].sum()
    campaign = df.groupby(["campaign_name", "keyword"], as_index=False)[metrics].sum()
    campaign_totals = df.groupby("campaign_name", as_index=False)[metrics].sum()

    if "avg_position" in df.columns:
        campaign_pos = (
            df.groupby(["campaign_name", "keyword"], as_index=False)["avg_position"]
            .mean()
        )
        campaign = campaign.merge(
            campaign_pos,
            on=["campaign_name", "keyword"],
            how="left",
        )

        account_pos = (
            campaign_pos.groupby("keyword", as_index=False)["avg_position"]
            .mean()
        )
        account = account.merge(account_pos, on="keyword", how="left")

    return PeriodData(
        raw=df,
        account_keyword=add_kpis(account),
        campaign_keyword=add_kpis(campaign),
        campaign_totals=campaign_totals,
    )


def calculate_account_totals(campaign_totals_dict: Dict[str, TotalsData]) -> TotalsData:
    """キャンペーン単位の合計値から、アカウント単位の合計値を計算"""
    total_imp = sum(t.imp for t in campaign_totals_dict.values())
    total_click = sum(t.click for t in campaign_totals_dict.values())
    total_cost = sum(t.cost for t in campaign_totals_dict.values())
    total_cv = sum(t.cv for t in campaign_totals_dict.values())

    return TotalsData(
        imp=total_imp,
        click=total_click,
        cost=total_cost,
        cv=total_cv,
    )


def get_campaign_totals_for_grouped_campaign(
    campaign_totals_dict: Dict[str, TotalsData],
    original_campaign_names: List[str],
) -> TotalsData:
    """合算されたキャンペーンの合計値を、合算前のキャンペーン名から集計"""
    total_imp = 0
    total_click = 0
    total_cost = 0
    total_cv = 0

    for original_name in original_campaign_names:
        if original_name in campaign_totals_dict:
            totals = campaign_totals_dict[original_name]
            total_imp += totals.imp
            total_click += totals.click
            total_cost += totals.cost
            total_cv += totals.cv

    return TotalsData(
        imp=total_imp,
        click=total_click,
        cost=total_cost,
        cv=total_cv,
    )


def get_original_campaigns_for_grouped(
    grouped_name: str,
    grouping_rules: List[Dict],
    original_df: pd.DataFrame,
) -> List[str]:
    """合算後のキャンペーン名に対応する合算前のキャンペーン名のリストを取得"""
    matching_rule = next((r for r in grouping_rules if r.get("name") == grouped_name), None)

    if matching_rule:
        keywords = matching_rule.get("include", [])
        if keywords:
            keyword = keywords[0]
            mask = original_df["campaign_name"].str.contains(keyword, na=False, regex=False)
            original_campaigns = original_df[mask]["campaign_name"].unique().tolist()
            return original_campaigns

    return [grouped_name]


# =====================
# Excel writer (run.py相当)
# =====================

# 列定義
NOW_COLUMNS = [
    "keyword",
    "imp", "click", "cost", "cv",
    "ctr", "cvr", "cpc", "cpa",
    "avg_position",
    "imp_share", "click_share", "cost_share", "cv_share",
]

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


def _sanitize_filename_component(s: str) -> str:
    """ファイル名に使いづらい文字を安全な文字に置換"""
    s = str(s)
    invalid = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
    for ch in invalid:
        s = s.replace(ch, "_")
    return s.strip()


def write_sheet_now_only(
    writer: pd.ExcelWriter,
    sheet_name: str,
    now_df: pd.DataFrame,
    avg_cpa: float = None,
    grouping_rules: list = None,
):
    """今期データのみを出力するシート"""
    workbook = writer.book

    # Excelのシート名は31文字制限
    MAX_SHEETNAME_LEN = 31

    def _ensure_len(name: str, max_len: int) -> str:
        name = str(name)
        return name if len(name) <= max_len else name[:max_len]

    actual_sheet_name = _ensure_len(sheet_name, MAX_SHEETNAME_LEN)
    worksheet = workbook.add_worksheet(actual_sheet_name)
    writer.sheets[actual_sheet_name] = worksheet

    percent_fmt = workbook.add_format({"num_format": "0.0%"})
    number_fmt = workbook.add_format({"num_format": "#,##0"})
    currency_fmt = workbook.add_format({"num_format": "¥#,##0"})
    position_fmt = workbook.add_format({"num_format": "0.0"})
    header_fmt = workbook.add_format({"bold": True, "bg_color": "#D3D3D3"})

    # 平均CPAを計算
    if avg_cpa is None:
        valid_mask = now_df["cv"] > 0
        if valid_mask.sum() > 0:
            total_cost = now_df.loc[valid_mask, "cost"].sum()
            total_cv = now_df.loc[valid_mask, "cv"].sum()
            avg_cpa = total_cost / total_cv if total_cv > 0 else 0
        else:
            avg_cpa = 0

    current_row = 0

    # 補足説明
    worksheet.write(current_row, 0, "CPA列の色分け：", header_fmt)
    current_row += 1
    worksheet.write(current_row, 0, "  平均CPA:", header_fmt)
    if avg_cpa > 0:
        worksheet.write(current_row, 1, avg_cpa, currency_fmt)
    else:
        worksheet.write(current_row, 1, "計算不可（CV=0のみ）")
    current_row += 1
    current_row += 1

    worksheet.write(current_row, 0, "  平均より-10%未満（良い）: 濃い緑")
    current_row += 1
    worksheet.write(current_row, 0, "  平均より-10%～-5%（やや良い）: 薄い緑")
    current_row += 1
    worksheet.write(current_row, 0, "  平均より-5%～+5%（標準）: 白")
    current_row += 1
    worksheet.write(current_row, 0, "  平均より+5%～+10%（やや悪い）: 薄い赤")
    current_row += 1
    worksheet.write(current_row, 0, "  平均より+10%超（悪い）: 濃い赤")
    current_row += 1
    worksheet.write(current_row, 0, "  CPA=0（CV獲得ゼロ）: グレー")
    current_row += 2

    current_col = 0

    for i, (title, sort_key, ascending) in enumerate(SORT_PATTERNS):
        worksheet.write(current_row, current_col, title)

        # ソート処理
        if sort_key == "cpa" and ascending:
            mask_valid = (now_df["cv"] > 0) & (now_df["cpa"] > 0) & (now_df["cpa"].notna())
            df_valid = now_df[mask_valid].sort_values(sort_key, ascending=ascending)
            df_invalid = now_df[~mask_valid]
            now_sorted = pd.concat([df_valid, df_invalid])
        else:
            now_sorted = now_df.sort_values(sort_key, ascending=ascending)

        # データ出力
        now_data = now_sorted.reset_index()[NOW_COLUMNS].copy()
        now_data.columns = [COLUMN_NAMES_JP.get(col, col) for col in now_data.columns]
        now_data.to_excel(
            writer,
            sheet_name=actual_sheet_name,
            startrow=current_row + 1,
            startcol=current_col,
            index=False,
        )

        # フォーマット設定
        for offset, col in enumerate(NOW_COLUMNS):
            col_idx = current_col + offset
            if col in PERCENT_COLUMNS:
                worksheet.set_column(col_idx, col_idx, None, percent_fmt)
            elif col == "avg_position":
                worksheet.set_column(col_idx, col_idx, None, position_fmt)
            elif col in ["imp", "click", "cv"]:
                worksheet.set_column(col_idx, col_idx, None, number_fmt)
            elif col in ["cost", "cpc", "cpa"]:
                worksheet.set_column(col_idx, col_idx, None, currency_fmt)

        # 条件付き書式（CPA列）
        cpa_col_idx = current_col + NOW_COLUMNS.index("cpa")
        data_start_row = current_row + 2
        data_end_row = data_start_row + len(now_sorted) - 1

        dark_green_fmt = workbook.add_format({"bg_color": "#C6EFCE"})
        light_green_fmt = workbook.add_format({"bg_color": "#E2EFDA"})
        light_red_fmt = workbook.add_format({"bg_color": "#FFC7CE"})
        dark_red_fmt = workbook.add_format({"bg_color": "#FF6B6B"})
        gray_fmt = workbook.add_format({"bg_color": "#D3D3D3"})

        base_cpa = avg_cpa

        if base_cpa > 0:
            worksheet.conditional_format(
                data_start_row, cpa_col_idx, data_end_row, cpa_col_idx,
                {"type": "cell", "criteria": "==", "value": 0, "format": gray_fmt}
            )
            worksheet.conditional_format(
                data_start_row, cpa_col_idx, data_end_row, cpa_col_idx,
                {"type": "cell", "criteria": "between", "minimum": 0.0001, "maximum": base_cpa * 0.9, "format": dark_green_fmt}
            )
            worksheet.conditional_format(
                data_start_row, cpa_col_idx, data_end_row, cpa_col_idx,
                {"type": "cell", "criteria": "between", "minimum": base_cpa * 0.9, "maximum": base_cpa * 0.95, "format": light_green_fmt}
            )
            worksheet.conditional_format(
                data_start_row, cpa_col_idx, data_end_row, cpa_col_idx,
                {"type": "cell", "criteria": "between", "minimum": base_cpa * 1.05, "maximum": base_cpa * 1.1, "format": light_red_fmt}
            )
            worksheet.conditional_format(
                data_start_row, cpa_col_idx, data_end_row, cpa_col_idx,
                {"type": "cell", "criteria": ">", "value": base_cpa * 1.1, "format": dark_red_fmt}
            )

        current_col += len(NOW_COLUMNS)

        if i < len(SORT_PATTERNS) - 1:
            worksheet.write(current_row + 1, current_col, "")
            current_col += 1


def write_summary_sheet(
    writer: pd.ExcelWriter,
    sheet_name: str,
    account_name: str,
    period_a_str: str,
    period_b_str: str,
    totals_a: TotalsData,
    totals_b: TotalsData,
    campaign_totals_a: Dict[str, TotalsData],
    campaign_totals_b: Dict[str, TotalsData],
    account_now: pd.DataFrame,
    account_prev: pd.DataFrame,
    use_grouping: bool = False,
    grouping_rules: list = None,
):
    """サマリーシートを出力"""
    workbook = writer.book
    worksheet = workbook.add_worksheet(sheet_name)
    writer.sheets[sheet_name] = worksheet

    percent_fmt = workbook.add_format({"num_format": "0.0%"})
    number_fmt = workbook.add_format({"num_format": "#,##0"})
    currency_fmt = workbook.add_format({"num_format": "¥#,##0"})
    header_fmt = workbook.add_format({"bold": True, "bg_color": "#D3D3D3"})
    section_header_fmt = workbook.add_format({"bold": True, "bg_color": "#E6E6FA", "font_size": 12})

    current_row = 0

    # 上部情報
    worksheet.write(current_row, 0, "アカウント名", header_fmt)
    worksheet.write(current_row, 1, account_name)
    current_row += 1

    worksheet.write(current_row, 0, "後期間", header_fmt)
    worksheet.write(current_row, 1, period_a_str)
    current_row += 1

    worksheet.write(current_row, 0, "前期間", header_fmt)
    worksheet.write(current_row, 1, period_b_str)
    current_row += 1

    worksheet.write(current_row, 0, "作成日時", header_fmt)
    worksheet.write(current_row, 1, datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
    current_row += 1

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
    current_row += 1

    # アカウント全体の指標
    worksheet.write(current_row, 0, "アカウント全体の指標", section_header_fmt)
    current_row += 1

    def calc_change_rate(a, b):
        if b == 0:
            return 0.0
        try:
            result = (a - b) / b
            import math
            if not math.isfinite(result):
                return 0.0
        except (ValueError, OverflowError, TypeError, ZeroDivisionError):
            return 0.0
        return result

    # 効率指標
    ctr_a = totals_a.click / totals_a.imp if totals_a.imp > 0 else 0
    cvr_a = totals_a.cv / totals_a.click if totals_a.click > 0 else 0
    cpc_a = totals_a.cost / totals_a.click if totals_a.click > 0 else 0
    cpa_a = totals_a.cost / totals_a.cv if totals_a.cv > 0 else 0

    ctr_b = totals_b.click / totals_b.imp if totals_b.imp > 0 else 0
    cvr_b = totals_b.cv / totals_b.click if totals_b.click > 0 else 0
    cpc_b = totals_b.cost / totals_b.click if totals_b.click > 0 else 0
    cpa_b = totals_b.cost / totals_b.cv if totals_b.cv > 0 else 0

    # ヘッダー
    headers = ["指標", period_a_str, period_b_str, "増減額", "増減率"]
    for col_idx, header in enumerate(headers):
        worksheet.write(current_row, col_idx, header, header_fmt)
    current_row += 1

    # データ
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
        worksheet.write(current_row, 0, metric)

        if row_idx < 4:
            if row_idx == 2:
                worksheet.write(current_row, 1, values_a[row_idx], currency_fmt)
                worksheet.write(current_row, 2, values_b[row_idx], currency_fmt)
                worksheet.write(current_row, 3, deltas[row_idx], currency_fmt)
            else:
                worksheet.write(current_row, 1, values_a[row_idx], number_fmt)
                worksheet.write(current_row, 2, values_b[row_idx], number_fmt)
                worksheet.write(current_row, 3, deltas[row_idx], number_fmt)
        else:
            if row_idx == 7:
                worksheet.write(current_row, 1, values_a[row_idx], currency_fmt)
                worksheet.write(current_row, 2, values_b[row_idx], currency_fmt)
                worksheet.write(current_row, 3, deltas[row_idx], currency_fmt)
            else:
                worksheet.write(current_row, 1, values_a[row_idx], percent_fmt)
                worksheet.write(current_row, 2, values_b[row_idx], percent_fmt)
                worksheet.write(current_row, 3, deltas[row_idx], percent_fmt)

        worksheet.write(current_row, 4, change_rates[row_idx], percent_fmt)
        current_row += 1

    worksheet.set_column(0, 0, 15)
    worksheet.set_column(1, 1, 15)
    worksheet.set_column(2, 2, 15)
    worksheet.set_column(3, 3, 15)
    worksheet.set_column(4, 4, 12)


# =====================
# Main処理
# =====================

def process_keyword_analysis(
    period_a_excel_path: str,
    period_b_excel_path: str,
    period_a_csv_path: str,
    period_b_csv_path: str,
    grouping_rules_text: str = "",
) -> bytes:
    """
    キーワード分析のメイン処理

    Args:
        period_a_excel_path: 後期間のExcelファイルパス
        period_b_excel_path: 前期間のExcelファイルパス
        period_a_csv_path: 後期間のCampaigns.csvファイルパス
        period_b_csv_path: 前期間のCampaigns.csvファイルパス
        grouping_rules_text: 合算ルール（カンマ区切り、例: "看護師,介護"）

    Returns:
        Excelファイルのバイトデータ
    """
    global USE_CAMPAIGN_GROUP, CAMPAIGN_GROUP_RULES

    # 合算ルールの解析
    grouping_rules = []
    if grouping_rules_text and grouping_rules_text.strip():
        keywords = [k.strip() for k in grouping_rules_text.split(',') if k.strip()]
        if keywords:
            for keyword in keywords:
                grouping_rules.append({
                    "name": keyword,
                    "include": [keyword],
                })
            USE_CAMPAIGN_GROUP = True
            CAMPAIGN_GROUP_RULES = grouping_rules
        else:
            USE_CAMPAIGN_GROUP = False
    else:
        USE_CAMPAIGN_GROUP = False

    # データ読み込み
    df_a = load_file_from_path(period_a_excel_path)
    df_b = load_file_from_path(period_b_excel_path)

    df_a_original = df_a.copy()
    df_b_original = df_b.copy()

    # 合算処理
    if USE_CAMPAIGN_GROUP and grouping_rules:
        df_a_grouped = apply_campaign_grouping(df_a.copy())
        df_b_grouped = apply_campaign_grouping(df_b.copy())

        period_a = build_period_data(df_a_grouped)
        period_b = build_period_data(df_b_grouped)

        period_a.raw = df_a_original
        period_b.raw = df_b_original
    else:
        period_a = build_period_data(df_a)
        period_b = build_period_data(df_b)

    # Campaigns.csv読み込み
    original_campaign_totals_a = load_campaign_totals_from_csv_path(period_a_csv_path)
    original_campaign_totals_b = load_campaign_totals_from_csv_path(period_b_csv_path)

    # 期間とアカウント名を抽出
    period_a_str, period_a_full_str = extract_period_from_csv_filename(Path(period_a_csv_path).name)
    period_b_str, period_b_full_str = extract_period_from_csv_filename(Path(period_b_csv_path).name)
    account_name = extract_account_name_from_filename(Path(period_a_csv_path).name)

    # 合算処理（campaign_totals）
    if USE_CAMPAIGN_GROUP and grouping_rules:
        campaign_totals_a = apply_grouping_to_campaign_totals(original_campaign_totals_a, grouping_rules)
        campaign_totals_b = apply_grouping_to_campaign_totals(original_campaign_totals_b, grouping_rules)
    else:
        campaign_totals_a = original_campaign_totals_a
        campaign_totals_b = original_campaign_totals_b

    totals_a = calculate_account_totals(campaign_totals_a)
    totals_b = calculate_account_totals(campaign_totals_b)

    # アカウント集計
    account_now = add_share(period_a.account_keyword, totals_a).set_index("keyword")
    account_prev = add_share(period_b.account_keyword, totals_b).set_index("keyword")

    # キャンペーン集計
    campaign_sheets = {}
    for campaign in period_a.campaign_keyword["campaign_name"].unique():
        a = period_a.campaign_keyword.query("campaign_name == @campaign")
        b = period_b.campaign_keyword.query("campaign_name == @campaign")

        if a.empty or b.empty:
            continue

        if USE_CAMPAIGN_GROUP:
            original_campaigns = get_original_campaigns_for_grouped(campaign, grouping_rules, period_a.raw)
            totals_a_c = get_campaign_totals_for_grouped_campaign(original_campaign_totals_a, original_campaigns)
            totals_b_c = get_campaign_totals_for_grouped_campaign(original_campaign_totals_b, original_campaigns)
        else:
            if campaign in original_campaign_totals_a and campaign in original_campaign_totals_b:
                totals_a_c = original_campaign_totals_a[campaign]
                totals_b_c = original_campaign_totals_b[campaign]
            else:
                totals_a_c = TotalsData(
                    imp=a["imp"].sum(),
                    click=a["click"].sum(),
                    cost=a["cost"].sum(),
                    cv=a["cv"].sum(),
                )
                totals_b_c = TotalsData(
                    imp=b["imp"].sum(),
                    click=b["click"].sum(),
                    cost=b["cost"].sum(),
                    cv=b["cv"].sum(),
                )

        now = add_share(a, totals_a_c).set_index("keyword")
        prev = add_share(b, totals_b_c).set_index("keyword")
        campaign_sheets[campaign] = (now, prev, totals_a_c, totals_b_c)

    # Excel生成（BytesIO）
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        # サマリーシート
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
            USE_CAMPAIGN_GROUP,
            grouping_rules,
        )

        # アカウントシート
        account_avg_cpa_a = totals_a.cost / totals_a.cv if totals_a.cv > 0 else 0
        account_avg_cpa_b = totals_b.cost / totals_b.cv if totals_b.cv > 0 else 0
        write_sheet_now_only(writer, f"アカウント_{period_a_str}", account_now, account_avg_cpa_a, grouping_rules)
        write_sheet_now_only(writer, f"アカウント_{period_b_str}", account_prev, account_avg_cpa_b, grouping_rules)

        # キャンペーンシート
        grouped_campaign_names = set()
        if USE_CAMPAIGN_GROUP and grouping_rules:
            for rule in grouping_rules:
                grouped_campaign_names.add(rule.get("name"))

        grouped_campaigns = []
        normal_campaigns = []

        for campaign, data in campaign_sheets.items():
            if campaign in grouped_campaign_names:
                grouped_campaigns.append((campaign, data))
            else:
                normal_campaigns.append((campaign, data))

        # 合算キャンペーン
        for campaign, (now_df, prev_df, totals_a_c, totals_b_c) in grouped_campaigns:
            invalid_chars = ['[', ']', ':', '*', '?', '/', '\\']
            base_name = campaign
            for char in invalid_chars:
                base_name = base_name.replace(char, '_')

            period_suffix_len = len(f"_{period_a_str}")
            max_base_len = 31 - period_suffix_len
            if len(base_name) > max_base_len:
                base_name = base_name[:max_base_len]

            campaign_avg_cpa_a = totals_a_c.cost / totals_a_c.cv if totals_a_c.cv > 0 else 0
            campaign_avg_cpa_b = totals_b_c.cost / totals_b_c.cv if totals_b_c.cv > 0 else 0

            write_sheet_now_only(writer, f"{base_name}_{period_a_str}", now_df, campaign_avg_cpa_a, grouping_rules)
            write_sheet_now_only(writer, f"{base_name}_{period_b_str}", prev_df, campaign_avg_cpa_b, grouping_rules)

        # 通常キャンペーン
        for campaign, (now_df, prev_df, totals_a_c, totals_b_c) in normal_campaigns:
            invalid_chars = ['[', ']', ':', '*', '?', '/', '\\']
            base_name = campaign
            for char in invalid_chars:
                base_name = base_name.replace(char, '_')

            period_suffix_len = len(f"_{period_a_str}")
            max_base_len = 31 - period_suffix_len
            if len(base_name) > max_base_len:
                base_name = base_name[:max_base_len]

            campaign_avg_cpa_a = totals_a_c.cost / totals_a_c.cv if totals_a_c.cv > 0 else 0
            campaign_avg_cpa_b = totals_b_c.cost / totals_b_c.cv if totals_b_c.cv > 0 else 0

            write_sheet_now_only(writer, f"{base_name}_{period_a_str}", now_df, campaign_avg_cpa_a, grouping_rules)
            write_sheet_now_only(writer, f"{base_name}_{period_b_str}", prev_df, campaign_avg_cpa_b, grouping_rules)

    output.seek(0)
    return output.getvalue()


# =====================
# Dify用エントリーポイント
# =====================

def main():
    """
    Dify Code実行ノードから呼び出される関数

    Dify側で以下の変数を設定:
    - period_a_excel_path: str
    - period_b_excel_path: str
    - period_a_csv_path: str
    - period_b_csv_path: str
    - grouping_rules_text: str (例: "看護師,介護")

    Dify出力:
    - result: bytes (Excelファイル)
    """
    # Difyの変数を受け取る（実際の実装ではDifyから渡される）
    # ここでは例として記載
    # period_a_excel_path = "/path/to/後期間.xlsx"
    # period_b_excel_path = "/path/to/前期間.xlsx"
    # period_a_csv_path = "/path/to/後期間_Campaigns.csv"
    # period_b_csv_path = "/path/to/前期間_Campaigns.csv"
    # grouping_rules_text = "看護師,介護"

    # 処理実行
    result = process_keyword_analysis(
        period_a_excel_path,
        period_b_excel_path,
        period_a_csv_path,
        period_b_csv_path,
        grouping_rules_text,
    )

    # Dify出力
    return {
        "result": result,
        "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "filename": "キーワードレポート.xlsx",
    }


if __name__ == "__main__":
    # テスト実行用
    print("Dify用キーワード分析スクリプトが読み込まれました")
