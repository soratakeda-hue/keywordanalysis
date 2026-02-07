from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Tuple, List

import pandas as pd
import config


# =====================
# Data classes
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
# Utilities
# =====================

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    mapped = {}
    for col in df.columns:
        normalized = str(col).strip().replace("　", " ")
        mapped[col] = config.COLUMN_MAP.get(normalized, normalized)
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
# Loaders
# =====================

def load_file(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(path, engine="openpyxl")
    else:
        df = pd.read_csv(path, encoding="utf-8-sig", thousands=",")

    df = normalize_columns(df)

    missing = sorted(config.REQUIRED_COLUMNS - set(df.columns))
    if missing:
        raise ValueError(f"{path.name} is missing required columns: {', '.join(missing)}")

    for col in config.NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = coerce_numeric(df[col])

    df["keyword"] = df["keyword"].astype(str)
    df["campaign_name"] = df["campaign_name"].astype(str)
    df["campaign_id"] = df["campaign_id"].astype(str)

    print(f"[LOAD] {path.name}")
    print("  columns:", df.columns.tolist())

    return df


def load_period(paths: Iterable[Path]) -> pd.DataFrame:
    frames = [load_file(p) for p in paths]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# =====================
# KPI calculations
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


def add_campaign_share(
    campaign_totals_dict: dict[str, TotalsData],
    account_totals: TotalsData,
) -> dict[str, dict[str, float]]:
    """
    キャンペーン単位のシェアを計算（アカウント全体に対する）
    
    Args:
        campaign_totals_dict: {キャンペーン名: TotalsData} の辞書
        account_totals: アカウント全体の合計値
    
    Returns:
        {キャンペーン名: {"imp_share": ..., "click_share": ..., "cost_share": ..., "cv_share": ...}} の辞書
    """
    campaign_shares = {}
    for campaign_name, campaign_totals in campaign_totals_dict.items():
        campaign_shares[campaign_name] = {
            "imp_share": safe_divide(campaign_totals.imp, account_totals.imp),
            "click_share": safe_divide(campaign_totals.click, account_totals.click),
            "cost_share": safe_divide(campaign_totals.cost, account_totals.cost),
            "cv_share": safe_divide(campaign_totals.cv, account_totals.cv),
        }
    return campaign_shares


# =====================
# Campaign grouping
# =====================

def apply_campaign_grouping(df: pd.DataFrame) -> pd.DataFrame:
    if not config.USE_CAMPAIGN_GROUP:
        return df

    df = df.copy()

    def map_campaign(name: str) -> str:
        # 各ルールのincludeキーワードを展開し、キーワードごとに別グループ化
        for rule in config.CAMPAIGN_GROUP_RULES:
            include = rule.get("include", [])
            exclude = rule.get("exclude", [])
            
            # exclude条件チェック
            if exclude and any(k in name for k in exclude):
                continue
            
            # include条件: 各キーワードごとに別グループ化
            # カンマ区切りで複数のキーワードが指定された場合、各キーワードごとに別々のグループを作成
            # 例: ["看護師", "介護"] → 「看護師を含む」と「介護を含む」でそれぞれ別グループ
            if include:
                # 最初にマッチしたキーワードのグループ名（キーワード文字列）を返す
                for keyword in include:
                    if keyword in name:
                        return keyword  # キーワード文字列をグループ名として使用
        
        return name

    df["campaign_name"] = df["campaign_name"].map(map_campaign)
    return df


def apply_grouping_to_campaign_totals(
    campaign_totals: dict[str, TotalsData],
    grouping_rules: list[dict],
) -> dict[str, TotalsData]:
    """
    キャンペーン合計データを合算後の名前で再集計する
    
    合算条件に該当するキャンペーンは合算後の名前でまとめ、
    該当しないキャンペーンは元の名前のまま保持する。
    
    Args:
        campaign_totals: 合算前のキャンペーン合計データ（外部ファイルから読み込んだ生データ）
        grouping_rules: 合算ルールのリスト [{"name": "合算後の名前", "include": ["キーワード1", "キーワード2"]}]
    
    Returns:
        合算後のキャンペーン合計データ
    """
    if not grouping_rules:
        return campaign_totals
    
    # キャンペーン名を合算後の名前に変換する関数
    def map_campaign(name: str) -> str:
        for rule in grouping_rules:
            include = rule.get("include", [])
            exclude = rule.get("exclude", [])
            
            # exclude条件: キーワードが含まれていないかチェック
            if exclude and any(k in name for k in exclude):
                continue
            
            # include条件: 各キーワードごとに別グループ化
            # カンマ区切りで複数のキーワードが指定された場合、各キーワードごとに別々のグループを作成
            # 例: ["看護師", "介護"] → 「看護師を含む」と「介護を含む」でそれぞれ別グループ
            if include:
                # 最初にマッチしたキーワードのグループ名（キーワード文字列）を返す
                for keyword in include:
                    if keyword in name:
                        return keyword  # キーワード文字列をグループ名として使用
        
        # どの条件にも該当しない場合、元の名前を返す
        return name
    
    # 合算後のキャンペーン名ごとに集計
    grouped_totals = {}
    for campaign_name, totals in campaign_totals.items():
        grouped_name = map_campaign(campaign_name)
        
        # 合算後の名前が初出の場合は初期化
        if grouped_name not in grouped_totals:
            grouped_totals[grouped_name] = TotalsData(
                imp=0.0,
                click=0.0,
                cost=0.0,
                cv=0.0,
            )
        
        # 合算（実績値を加算）
        grouped_totals[grouped_name].imp += totals.imp
        grouped_totals[grouped_name].click += totals.click
        grouped_totals[grouped_name].cost += totals.cost
        grouped_totals[grouped_name].cv += totals.cv
    
    return grouped_totals


# =====================
# Period builder
# =====================

def build_period_data(df: pd.DataFrame) -> PeriodData:
    df = apply_campaign_grouping(df)

    metrics = ["imp", "click", "cost", "cv"]

    account = df.groupby("keyword", as_index=False)[metrics].sum()
    campaign = df.groupby(["campaign_name", "keyword"], as_index=False)[metrics].sum()
    campaign_totals = df.groupby("campaign_name", as_index=False)[metrics].sum()

    # 掲載順位（平均表示順位）
    # - キャンペーン×KW: 平均（通常は1行なので実質そのまま）
    # - アカウント×KW: キャンペーン単位の平均表示順位を単純平均（= キャンペーンを1票）
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


# =====================
# Share delta (A - B)
# =====================

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
# Totals file loading
# =====================

def load_campaign_totals_from_csv(csv_path: Path) -> dict[str, TotalsData]:
    """
    CSVファイルからキャンペーン単位の合計値を読み込む
    
    Args:
        csv_path: CSVファイルのパス
    
    Returns:
        {キャンペーン名: TotalsData} の辞書
    """
    # CSVファイルを読み込む（Shift-JISエンコーディング）
    df = pd.read_csv(csv_path, encoding="shift_jis")
    
    # カラム名をマッピング（日本語→英語）
    column_mapping = {
        "キャンペーン名": "campaign_name",
        "表示回数": "imp",
        "クリック数": "click",
        "消化予算": "cost",
        "応募数": "cv",
    }
    
    # カラム名を正規化
    df_renamed = df.rename(columns=column_mapping)
    
    # 必要なカラムが存在するか確認
    required_cols = ["campaign_name", "imp", "click", "cost", "cv"]
    missing = [col for col in required_cols if col not in df_renamed.columns]
    if missing:
        raise ValueError(f"{csv_path.name} is missing required columns: {', '.join(missing)}")
    
    # campaign_name の正規化
    df_renamed["campaign_name"] = df_renamed["campaign_name"].astype(str).str.strip()

    # 合計行/空行を除外（集計が欠ける原因になりやすい）
    df_renamed = df_renamed[
        df_renamed["campaign_name"].notna()
        & (df_renamed["campaign_name"] != "")
        & (df_renamed["campaign_name"] != "合計")
    ].copy()

    # 数値列を数値型に変換（'-'などの文字列を0に）
    for col in ["imp", "click", "cost", "cv"]:
        df_renamed[col] = pd.to_numeric(df_renamed[col], errors="coerce").fillna(0).astype(float)

    # 同一キャンペーンが複数行ある場合に備えて合算
    grouped = (
        df_renamed.groupby("campaign_name", as_index=False)[["imp", "click", "cost", "cv"]]
        .sum()
    )

    result: dict[str, TotalsData] = {}
    for _, row in grouped.iterrows():
        campaign_name = str(row["campaign_name"]).strip()
        result[campaign_name] = TotalsData(
            imp=float(row["imp"]),
            click=float(row["click"]),
            cost=float(row["cost"]),
            cv=float(row["cv"]),
        )

    return result


def detect_period_from_filename(filename: str) -> str:
    """
    ファイル名から期間を判定
    
    Args:
        filename: ファイル名（例: "StanbyAD-Report_..._20260101-20260131_Campaigns.csv"）
    
    Returns:
        "後期間" または "前期間"
    
    判定方法:
    - ファイル名に含まれる日付範囲（YYYYMMDD-YYYYMMDD）から終了日を取得
    - より新しい日付の方を「後期間」とする
    """
    import re
    from datetime import datetime
    
    # 日付範囲のパターンを検索（YYYYMMDD-YYYYMMDD）
    pattern = r"(\d{8})-(\d{8})"
    match = re.search(pattern, filename)
    
    if not match:
        raise ValueError(f"ファイル名から日付範囲を検出できません: {filename}")
    
    start_date_str = match.group(1)
    end_date_str = match.group(2)
    
    try:
        end_date = datetime.strptime(end_date_str, "%Y%m%d")
        # 終了日が2025年1月以降なら「後期間」、それ以前なら「前期間」と仮定
        # より正確には、2つのファイルを比較して新しい方を「後期間」とする
        # ここでは簡易的に2025年1月1日を境界とする
        threshold = datetime(2025, 1, 1)
        if end_date >= threshold:
            return "後期間"
        else:
            return "前期間"
    except ValueError:
        raise ValueError(f"日付の解析に失敗しました: {end_date_str}")


def extract_account_name_from_filename(filename: str) -> str:
    """
    ファイル名からアカウント名を抽出
    
    例: "StanbyAD-Report_株式会社メドレー_ジョブメドレー_すべて_20260101-20260131_Campaigns.csv"
    → "株式会社メドレー_ジョブメドレー"
    
    Args:
        filename: CSVファイル名
    
    Returns:
        アカウント名
    """
    import re
    
    start_marker = "Report_"
    
    # Report_の位置を検索
    start_idx = filename.find(start_marker)
    if start_idx == -1:
        raise ValueError(f"ファイル名の形式が正しくありません（Report_が見つかりません）: {filename}")
    
    start_idx += len(start_marker)
    
    # _すべての位置を検索
    end_marker = "_すべて"
    end_idx = filename.find(end_marker, start_idx)
    
    if end_idx == -1:
        raise ValueError(f"ファイル名から「_すべて」を検出できません: {filename}")
    
    if end_idx <= start_idx:
        raise ValueError(f"ファイル名からアカウント名を抽出できません: {filename}")
    
    # Report_直後から_すべてまでの文字列を抽出
    account_name = filename[start_idx:end_idx]
    return account_name


def load_totals_files(
    totals_dir: Path,
) -> Tuple[
    dict[str, TotalsData],
    dict[str, TotalsData],
    str,
    str,
    str,
    str,
    str,
]:
    """
    totalsディレクトリからCSVファイルを自動検出・読み込む
    
    Args:
        totals_dir: totalsディレクトリのパス
    
    Returns:
        (
          後期間のキャンペーン合計値辞書,
          前期間のキャンペーン合計値辞書,
          アカウント名,
          後期間文字列（mmdd-mmdd）,
          前期間文字列（mmdd-mmdd）,
          後期間文字列（YYYYMMDD-YYYYMMDD）,
          前期間文字列（YYYYMMDD-YYYYMMDD）,
        )
    """
    if not totals_dir.exists():
        raise FileNotFoundError(f"totalsディレクトリが見つかりません: {totals_dir}")
    
    # CSVファイルを検出
    csv_files = [
        p for p in totals_dir.iterdir()
        if p.is_file() and p.suffix.lower() == ".csv" and "Campaigns" in p.name
    ]
    
    if not csv_files:
        raise ValueError(f"totalsディレクトリにCSVファイルが見つかりません: {totals_dir}")
    
    # ファイル名から日付を抽出してソート
    import re
    from datetime import datetime
    
    file_dates = []
    for csv_file in csv_files:
        pattern = r"(\d{8})-(\d{8})"
        match = re.search(pattern, csv_file.name)
        if match:
            end_date_str = match.group(2)
            try:
                end_date = datetime.strptime(end_date_str, "%Y%m%d")
                file_dates.append((end_date, csv_file))
            except ValueError:
                continue
    
    if len(file_dates) < 2:
        raise ValueError(f"期間判定のためには2つのCSVファイルが必要です（現在: {len(file_dates)}個）")
    
    # 日付でソート（新しい方が後期間）
    file_dates.sort(key=lambda x: x[0])
    period_b_file = file_dates[0][1]  # 古い方（前期間）
    period_a_file = file_dates[-1][1]  # 新しい方（後期間）
    
    # ファイルを読み込む
    period_a_dict = load_campaign_totals_from_csv(period_a_file)
    period_b_dict = load_campaign_totals_from_csv(period_b_file)
    
    # アカウント名を抽出（後期間のファイルから）
    account_name = extract_account_name_from_filename(period_a_file.name)
    
    # 期間情報を抽出（mmdd-mmdd形式 + YYYYMMDD-YYYYMMDD形式）
    pattern = r"(\d{8})-(\d{8})"
    match_a = re.search(pattern, period_a_file.name)
    match_b = re.search(pattern, period_b_file.name)
    
    if match_a and match_b:
        start_a = datetime.strptime(match_a.group(1), "%Y%m%d")
        end_a = datetime.strptime(match_a.group(2), "%Y%m%d")
        start_b = datetime.strptime(match_b.group(1), "%Y%m%d")
        end_b = datetime.strptime(match_b.group(2), "%Y%m%d")
        
        period_a_str = f"{start_a.strftime('%m%d')}-{end_a.strftime('%m%d')}"
        period_b_str = f"{start_b.strftime('%m%d')}-{end_b.strftime('%m%d')}"
        period_a_full_str = f"{match_a.group(1)}-{match_a.group(2)}"
        period_b_full_str = f"{match_b.group(1)}-{match_b.group(2)}"
    else:
        # フォールバック（通常は発生しない）
        period_a_str = "後期間"
        period_b_str = "前期間"
        period_a_full_str = "後期間"
        period_b_full_str = "前期間"
    
    return (
        period_a_dict,
        period_b_dict,
        account_name,
        period_a_str,
        period_b_str,
        period_a_full_str,
        period_b_full_str,
    )


def calculate_account_totals(campaign_totals_dict: dict[str, TotalsData]) -> TotalsData:
    """
    キャンペーン単位の合計値から、アカウント単位の合計値を計算
    
    Args:
        campaign_totals_dict: {キャンペーン名: TotalsData} の辞書
    
    Returns:
        全キャンペーンの合計値
    """
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
    campaign_totals_dict: dict[str, TotalsData],
    original_campaign_names: List[str],
) -> TotalsData:
    """
    合算されたキャンペーンの合計値を、合算前のキャンペーン名から集計する
    
    Args:
        campaign_totals_dict: 外部ファイルから読み込んだ合計値辞書（合算前のキャンペーン名をキー）
        original_campaign_names: この合算グループに属する元のキャンペーン名のリスト
    
    Returns:
        集計された合計値
    """
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


# =====================
# File discovery
# =====================

def load_period_files(input_dir: Path) -> Tuple[List[Path], List[Path]]:
    """
    前期間と後期間のフォルダからファイルを読み込む
    
    Args:
        input_dir: データディレクトリ（前期間/後期間フォルダを含む）
    
    Returns:
        (後期間ファイルリスト, 前期間ファイルリスト)
    """
    period_a_dir = input_dir / "後期間"  # 今期
    period_b_dir = input_dir / "前期間"  # 前期
    
    period_a = []
    period_b = []
    
    if period_a_dir.exists() and period_a_dir.is_dir():
        period_a = [
            p for p in period_a_dir.iterdir()
            if p.is_file() 
            and p.suffix.lower() in {'.xlsx', '.xls', '.csv'}
            and not p.name.startswith('.')  # .DS_Storeなどを除外
        ]
    
    if period_b_dir.exists() and period_b_dir.is_dir():
        period_b = [
            p for p in period_b_dir.iterdir()
            if p.is_file() 
            and p.suffix.lower() in {'.xlsx', '.xls', '.csv'}
            and not p.name.startswith('.')  # .DS_Storeなどを除外
        ]
    
    if not period_a:
        raise ValueError(f"後期間のフォルダが見つからないか、ファイルがありません: {period_a_dir}")
    if not period_b:
        raise ValueError(f"前期間のフォルダが見つからないか、ファイルがありません: {period_b_dir}")
    
    return period_a, period_b
