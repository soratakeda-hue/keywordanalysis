#!/usr/bin/env python3
import argparse
from pathlib import Path

import pandas as pd

import analyzer
import config
from run import (  # run.pyから必要な関数と定数をインポート
    parse_args,
    _make_unique_path,
    _sanitize_filename_component,
    write_sheet_now_only,
    write_sheet_with_3sort_blocks,
    write_summary_sheet,
    NOW_COLUMNS,
    DELTA_COLUMNS,
    SORT_PATTERNS,
    PERCENT_COLUMNS,
    COLUMN_NAMES_JP,
)


def ask_grouping_setup():
    """
    実行時にキャンペーン合算機能の設定を尋ねる
    
    カンマ区切りで複数のキーワードを入力した場合、各キーワードごとに別々のグループを作成する
    
    Returns:
        tuple: (use_grouping: bool, grouping_rules: list[dict])
    """
    print("\n=== キャンペーン合算機能の設定 ===")
    grouping_rules = []
    
    try:
        keywords_input = input("合算するキーワード条件を入力してください（カンマ区切り、例: 看護師,介護）: ").strip()
        
        if keywords_input:
            keywords = [k.strip() for k in keywords_input.split(',') if k.strip()]
            if keywords:
                # 各キーワードごとに別々のルールを作成
                # 例: ["看護師", "介護"] → [{"name": "看護師", "include": ["看護師"]}, {"name": "介護", "include": ["介護"]}]
                for keyword in keywords:
                    grouping_rule = {
                        "name": keyword,  # キーワード文字列をグループ名として使用
                        "include": [keyword],  # 各キーワードごとに別グループ
                    }
                    grouping_rules.append(grouping_rule)
                
                config.USE_CAMPAIGN_GROUP = True
                config.CAMPAIGN_GROUP_RULES = grouping_rules
                print(f"\n✓ 合算ルールを設定しました: {', '.join(keywords)}")
                print(f"  各キーワードごとに別々のグループとして合算されます。")
            else:
                config.USE_CAMPAIGN_GROUP = False
                print("\n✓ キーワードが入力されていないため、合算機能を無効化しました。")
        else:
            config.USE_CAMPAIGN_GROUP = False
            print("\n✓ 入力が空のため、合算機能を無効化しました。")
    except (EOFError, KeyboardInterrupt):
        config.USE_CAMPAIGN_GROUP = False
        print("\n入力が中断されました。合算機能を無効化しました。")
    
    use_grouping = len(grouping_rules) > 0
    
    print("\n" + "="*50 + "\n")
    return use_grouping, grouping_rules


def get_original_campaigns_for_grouped(
    grouped_name: str,
    grouping_rules: list[dict],
    original_df: pd.DataFrame,
) -> list[str]:
    """
    合算後のキャンペーン名に対応する合算前のキャンペーン名のリストを取得
    
    Args:
        grouped_name: 合算後のキャンペーン名（キーワード文字列、例: "看護師"）
        grouping_rules: 合算ルールのリスト
        original_df: 合算前のデータフレーム
    
    Returns:
        合算前のキャンペーン名のリスト
    """
    # 合算後の名前（キーワード文字列）が合算ルールに含まれているかチェック
    matching_rule = next((r for r in grouping_rules if r.get("name") == grouped_name), None)
    
    if matching_rule:
        # 合算ルールに該当する場合、キーワード条件で合算前のキャンペーン名を抽出
        keywords = matching_rule.get("include", [])
        if keywords:
            # キーワードを含むキャンペーン名を抽出（各キーワードごとに別グループ化されているため、1つのキーワードのみをチェック）
            keyword = keywords[0]  # 各ルールには1つのキーワードのみが含まれる
            mask = original_df["campaign_name"].str.contains(keyword, na=False, regex=False)
            original_campaigns = original_df[mask]["campaign_name"].unique().tolist()
            return original_campaigns
    
    # 合算ルールに該当しない場合（合算されていないキャンペーン）、そのまま返す
    return [grouped_name]


def main() -> None:
    # CLI引数の解析
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # ---------- キャンペーン合算機能の設定（実行時に尋ねる） ----------
    use_grouping, grouping_rules = ask_grouping_setup()

    # ---------- load ----------
    period_a_files, period_b_files = analyzer.load_period_files(args.input_dir)
    df_a = analyzer.load_period(period_a_files)
    df_b = analyzer.load_period(period_b_files)
    
    # 合算前のデータを明示的に保持（period_a.rawに設定するため）
    df_a_original = df_a.copy()
    df_b_original = df_b.copy()

    # 合算処理を明示的に実行
    if use_grouping and grouping_rules:
        # 合算処理を実行（build_period_data内で実行されるが、ここでも明示的に実行）
        df_a_grouped = analyzer.apply_campaign_grouping(df_a.copy())
        df_b_grouped = analyzer.apply_campaign_grouping(df_b.copy())
        
        # PeriodDataを作成（合算後のデータで）
        period_a = analyzer.build_period_data(df_a_grouped)
        period_b = analyzer.build_period_data(df_b_grouped)
        
        # rawに合算前のデータを明示的に保持（既存ロジックとの互換性のため）
        period_a.raw = df_a_original
        period_b.raw = df_b_original
    else:
        # 合算しない場合は通常の処理
        period_a = analyzer.build_period_data(df_a)
        period_b = analyzer.build_period_data(df_b)

    # ---------- totals (外部ファイルから読み込み) ----------
    totals_dir = args.input_dir / "totals"
    original_campaign_totals_a, original_campaign_totals_b, account_name, period_a_str, period_b_str, period_a_full_str, period_b_full_str = analyzer.load_totals_files(totals_dir)
    
    # 合算機能を使っている場合、サマリーシート用にcampaign_totalsを合算後の名前で再集計
    # キャンペーンシート生成には合算前のoriginal_campaign_totalsを使用する
    if use_grouping and grouping_rules:
        campaign_totals_a = analyzer.apply_grouping_to_campaign_totals(original_campaign_totals_a, grouping_rules)
        campaign_totals_b = analyzer.apply_grouping_to_campaign_totals(original_campaign_totals_b, grouping_rules)
    else:
        campaign_totals_a = original_campaign_totals_a
        campaign_totals_b = original_campaign_totals_b
    
    # アカウント名と期間を表示
    print(f"アカウント名: {account_name}")
    print(f"後期間: {period_a_str}")
    print(f"前期間: {period_b_str}")
    
    # アカウント単位の合計値はキャンペーン単位から合算（合算後のcampaign_totalsを使用）
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

        if a.empty:
            continue  # 後期間にデータなし → スキップ
        is_b_only = b.empty  # 後期間のみフラグ

        # 外部ファイルからキャンペーン単位の合計値を取得
        # 合算が有効な場合、合算前のキャンペーン名で取得する必要がある
        if config.USE_CAMPAIGN_GROUP:
            # 合算前のキャンペーン名を取得（合算前のデータから逆引き）
            original_campaigns = get_original_campaigns_for_grouped(
                campaign,
                grouping_rules,
                period_a.raw,  # 合算前のデータ（明示的に設定済み）
            )
            
            # 合算前のキャンペーン名の合計値を集計
            totals_a_c = analyzer.get_campaign_totals_for_grouped_campaign(
                original_campaign_totals_a,  # 合算前のcampaign_totalsを使用
                original_campaigns,
            )
            totals_b_c = analyzer.get_campaign_totals_for_grouped_campaign(
                original_campaign_totals_b,  # 合算前のcampaign_totalsを使用
                original_campaigns,
            )
        else:
            # 合算が無効な場合、キャンペーン名で直接突合
            # 後期間totals
            if campaign in original_campaign_totals_a:
                totals_a_c = original_campaign_totals_a[campaign]
            else:
                totals_a_c = analyzer.TotalsData(
                    imp=a["imp"].sum(),
                    click=a["click"].sum(),
                    cost=a["cost"].sum(),
                    cv=a["cv"].sum(),
                )
            # 前期間totals: 後期間のみキャンペーンはゼロ埋め
            if is_b_only:
                totals_b_c = analyzer.TotalsData(imp=0.0, click=0.0, cost=0.0, cv=0.0)
            elif campaign in original_campaign_totals_b:
                totals_b_c = original_campaign_totals_b[campaign]
            else:
                totals_b_c = analyzer.TotalsData(
                    imp=b["imp"].sum(),
                    click=b["click"].sum(),
                    cost=b["cost"].sum(),
                    cv=b["cv"].sum(),
                )

        now = analyzer.add_share(a, totals_a_c).set_index("keyword")

        if is_b_only:
            prev = pd.DataFrame()
            delta = pd.DataFrame()
        else:
            prev = analyzer.add_share(b, totals_b_c).set_index("keyword")
            delta = analyzer.add_share_delta(
                a,
                b,
                totals_a_c,
                totals_b_c,
                keys=["campaign_name", "keyword"],
            ).set_index("keyword")

        campaign_sheets[campaign] = (now, prev, delta, totals_a_c, totals_b_c, is_b_only)

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
            campaign_totals_a,  # 合算後のcampaign_totals
            campaign_totals_b,  # 合算後のcampaign_totals
            account_now,
            account_prev,
            use_grouping,  # 合算情報を渡す
            grouping_rules,  # 合算ルールを渡す
        )
        
        # アカウント: 今期と前期を別シートに
        account_avg_cpa_a = totals_a.cost / totals_a.cv if totals_a.cv > 0 else 0
        account_avg_cpa_b = totals_b.cost / totals_b.cv if totals_b.cv > 0 else 0
        write_sheet_now_only(writer, f"アカウント_{period_a_str}", account_now, account_avg_cpa_a, grouping_rules)
        write_sheet_now_only(writer, f"アカウント_{period_b_str}", account_prev, account_avg_cpa_b, grouping_rules)
        
        # キャンペーン: 今期と前期を別シートに
        # 合算されたキャンペーンを先に作成し、その後通常のキャンペーンを配置
        grouped_campaign_names = set()
        if use_grouping and grouping_rules:
            # 合算されたキャンペーン名のセットを作成
            for rule in grouping_rules:
                grouped_campaign_names.add(rule.get("name"))
        
        # 合算されたキャンペーンと通常のキャンペーンを分ける
        grouped_campaigns = []
        normal_campaigns = []
        
        for campaign, data in campaign_sheets.items():
            if campaign in grouped_campaign_names:
                grouped_campaigns.append((campaign, data))
            else:
                normal_campaigns.append((campaign, data))
        
        # 合算されたキャンペーンを先に作成
        for campaign, (now_df, prev_df, delta_df, totals_a_c, totals_b_c, is_b_only) in grouped_campaigns:
            # Excelのシート名に使用できない文字を除去
            invalid_chars = ['[', ']', ':', '*', '?', '/', '\\']
            base_name = campaign
            for char in invalid_chars:
                base_name = base_name.replace(char, '_')

            # 期間文字列（例：0101-0131）は9文字、Excelのシート名は31文字制限
            period_suffix_len = len(f"_{period_a_str}")
            max_base_len = 31 - period_suffix_len
            if len(base_name) > max_base_len:
                base_name = base_name[:max_base_len]

            # キャンペーン全体のCPA平均を計算（加重平均）
            campaign_avg_cpa_a = totals_a_c.cost / totals_a_c.cv if totals_a_c.cv > 0 else 0

            write_sheet_now_only(writer, f"{base_name}_{period_a_str}", now_df, campaign_avg_cpa_a, grouping_rules)
            if not is_b_only:
                campaign_avg_cpa_b = totals_b_c.cost / totals_b_c.cv if totals_b_c.cv > 0 else 0
                write_sheet_now_only(writer, f"{base_name}_{period_b_str}", prev_df, campaign_avg_cpa_b, grouping_rules)

        # 通常のキャンペーンを作成
        for campaign, (now_df, prev_df, delta_df, totals_a_c, totals_b_c, is_b_only) in normal_campaigns:
            # Excelのシート名に使用できない文字を除去
            invalid_chars = ['[', ']', ':', '*', '?', '/', '\\']
            base_name = campaign
            for char in invalid_chars:
                base_name = base_name.replace(char, '_')

            # 期間文字列（例：0101-0131）は9文字、Excelのシート名は31文字制限
            period_suffix_len = len(f"_{period_a_str}")
            max_base_len = 31 - period_suffix_len
            if len(base_name) > max_base_len:
                base_name = base_name[:max_base_len]

            # キャンペーン全体のCPA平均を計算（加重平均）
            campaign_avg_cpa_a = totals_a_c.cost / totals_a_c.cv if totals_a_c.cv > 0 else 0

            write_sheet_now_only(writer, f"{base_name}_{period_a_str}", now_df, campaign_avg_cpa_a, grouping_rules)
            if not is_b_only:
                campaign_avg_cpa_b = totals_b_c.cost / totals_b_c.cv if totals_b_c.cv > 0 else 0
                write_sheet_now_only(writer, f"{base_name}_{period_b_str}", prev_df, campaign_avg_cpa_b, grouping_rules)

    print(f"Excel 出力完了: {output_path}")


if __name__ == "__main__":
    main()
