# =====================
# Column mapping
# 日本語カラム名 → 内部共通名
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


# =====================
# Required columns
# analyzer が最低限必要とする列
# =====================

REQUIRED_COLUMNS = {
    "keyword",
    "campaign_id",
    "campaign_name",
    "imp",
    "click",
    "cost",
    "cv",
}


# =====================
# Numeric columns
# 数値として変換する列
# =====================

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


# =====================
# Ranking settings
# =====================

TOP_N_PER_CAMPAIGN = 50


# =====================
# Notice text (output/NOTICE.txt)
# =====================

NOTICE_TEXT = (
    "このレポートはワード単位で合算したレポートであり、MECEではありません。\n"
    "例：「介護士 正社員」で広告が表示された場合、\n"
    "「介護士」「正社員」の双方に1表示が計上される可能性があります。\n"
    "そのため、アカウント／キャンペーンに対する割合は参考値として扱ってください。"
)


# =====================
# Optional totals file names
# =====================

TOTALS_FILENAME_A = "totals_A.csv"
TOTALS_FILENAME_B = "totals_B.csv"


# =====================
# Campaign grouping (optional)
# キャンペーン名を文字列条件で合算するための設定
# =====================

# True にしたときのみ、キャンペーン合算ロジックを有効化
USE_CAMPAIGN_GROUP = False


# 合算ルール定義
# - name    : 合算後のキャンペーン名
# - include : 含む文字列（OR条件）
# - exclude : 除外する文字列（OR条件）
#
# ※ include / exclude はどちらか一方だけでもOK
# ※ 意味（看護師など）は analyzer / run.py 側では一切解釈しない

CAMPAIGN_GROUP_RULES = [
    {
        "name": "include_example",
        "include": ["看護師"],
    },
    {
        "name": "exclude_example",
        "exclude": ["看護師"],
    },
]
