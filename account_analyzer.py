#!/usr/bin/env python3
"""
アカウント分析くん
指定フォルダからファイルを検出し、適切なフォルダに移動してから分析を実行します。
"""
import argparse
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import run_grouping  # 合算対応版の実行エントリポイント


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-source", type=Path, default=None, help="データソースフォルダのパス（指定なしでDownloads使用）")
    return parser.parse_args()


def get_downloads_folder() -> Path:
    """Downloadsフォルダのパスを取得"""
    home = Path.home()
    downloads = home / "Downloads"
    return downloads


def find_campaigns_csv_files(downloads_dir: Path) -> List[Path]:
    """DownloadsフォルダからCampaigns.csvファイルを検出（更新日時でソート：新しい順）"""
    csv_files = [
        p for p in downloads_dir.iterdir()
        if p.is_file()
        and p.suffix.lower() == ".csv"
        and "Campaigns" in p.name
        and re.search(r"\d{8}-\d{8}", p.name)  # 日付範囲を含む
    ]
    # 更新日時でソート（新しい順）
    csv_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return csv_files


def find_keyword_excel_files(downloads_dir: Path) -> List[Path]:
    """Downloadsフォルダからキーワードデータのファイル（Excel/CSV）を検出
    「キーワード別レポート」を含むファイルのみを返す（更新日時でソート：新しい順）"""
    all_files = [
        p for p in downloads_dir.iterdir()
        if p.is_file()
        and p.suffix.lower() in {".xlsx", ".xls", ".csv"}  # CSV cache
        and not p.name.startswith(".")  # .DS_Storeなどを除外
        and not p.name.startswith("~")  # 一時ファイルを除外
        and "Campaigns" not in p.name  # Campaigns.csvは除外（totals用）
    ]
    
    # 「キーワード別レポート」を含むファイルのみを返す
    keyword_files = [f for f in all_files if "キーワード別レポート" in f.name]
    
    # 更新日時でソート（新しい順）
    keyword_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    
    return keyword_files


def extract_date_range(filename: str) -> Tuple[datetime, datetime]:
    """ファイル名から日付範囲を抽出"""
    pattern = r"(\d{8})-(\d{8})"
    match = re.search(pattern, filename)
    if not match:
        raise ValueError(f"ファイル名から日付範囲を検出できません: {filename}")
    
    start_date_str = match.group(1)
    end_date_str = match.group(2)
    
    start_date = datetime.strptime(start_date_str, "%Y%m%d")
    end_date = datetime.strptime(end_date_str, "%Y%m%d")
    
    return start_date, end_date


def extract_account_name(filename: str) -> str:
    """ファイル名からアカウント名を抽出"""
    # StanbyAD-Report_アカウント名_すべて_日付_Campaigns.csv の形式から抽出
    pattern = r"StanbyAD-Report_(.+?)_すべて_\d{8}-\d{8}_Campaigns"
    match = re.search(pattern, filename)
    if match:
        return match.group(1)
    return "不明"


def group_campaigns_by_account(csv_files: List[Path]) -> dict:
    """Campaigns.csvファイルをアカウントごとにグループ化"""
    grouped = {}
    for csv_file in csv_files:
        try:
            account_name = extract_account_name(csv_file.name)
            if account_name not in grouped:
                grouped[account_name] = []
            grouped[account_name].append(csv_file)
        except Exception:
            if "不明" not in grouped:
                grouped["不明"] = []
            grouped["不明"].append(csv_file)
    return grouped


def select_campaigns_files(csv_files: List[Path]) -> List[Path]:
    """ユーザーにCampaigns.csvファイルの選択を求める（キーワード別レポートと同じ形式）"""
    if len(csv_files) < 2:
        print(f"⚠ Campaigns.csvファイルが2つ以上必要です（現在: {len(csv_files)}個）")
        return []
    
    # アカウントごとにグループ化（情報表示用）
    grouped = group_campaigns_by_account(csv_files)
    
    # アカウント名とファイルのマッピングを作成
    file_to_account = {}
    for account_name, files in grouped.items():
        for f in files:
            file_to_account[f] = account_name
    
    print("\n[Campaigns.csvファイルの選択]")
    print("=" * 60)
    print(f"検出されたCampaigns.csvファイル: {len(csv_files)}個\n")
    
    # ファイルを更新日時順でソートして表示
    file_list = []
    for csv_file in csv_files:
        try:
            start_date, end_date = extract_date_range(csv_file.name)
            # 期間の終了日を日付文字列として使用
            date_str = end_date.strftime("%Y/%m/%d")
            account_name = file_to_account.get(csv_file, "不明")
            # 更新日時を追加（ソート用）
            mtime = csv_file.stat().st_mtime
            file_list.append((date_str, csv_file, account_name, start_date, end_date, mtime))
        except ValueError:
            # 日付範囲が取得できない場合はスキップ
            continue
    
    # 更新日時でソート（新しい順）
    file_list.sort(key=lambda x: x[5], reverse=True)  # x[5]はmtime（更新日時）
    
    # 一覧表示（最大50個まで）
    display_count = min(50, len(file_list))
    print("\n【Campaigns.csv】")
    for i, (date_str, csv_file, account_name, start_date, end_date, mtime) in enumerate(file_list[:display_count], 1):
        size_mb = csv_file.stat().st_size / (1024 * 1024)
        period_str = f"{start_date.strftime('%Y/%m/%d')} ～ {end_date.strftime('%Y/%m/%d')}"
        print(f"{i:3d}. [{date_str}] {csv_file.name}")
        print(f"     {account_name} | {period_str} | {size_mb:.1f}MB")
    
    if len(file_list) > display_count:
        print(f"\n... 他 {len(file_list) - display_count}個のファイル")
    
    print("\n" + "=" * 60)
    print("使用するファイルの番号を入力してください（2つ以上選択、カンマ区切り）:")
    print("例: 1,3 または 1-3（範囲指定）")
    user_input = input("> ").strip()
    
    selected_files = []
    if not user_input:
        print("⚠ ファイルが選択されませんでした。スキップします。")
        return []
    
    try:
        for part in user_input.split(","):
            part = part.strip()
            if "-" in part:
                # 範囲指定
                start, end = part.split("-")
                selected_indices = range(int(start.strip()), int(end.strip()) + 1)
            else:
                selected_indices = [int(part.strip())]
            
            for idx in selected_indices:
                if 1 <= idx <= display_count:
                    selected_files.append(file_list[idx - 1][1])  # [1]はcsv_file
                else:
                    print(f"⚠ 番号 {idx} は無効です（1-{display_count}の範囲で指定してください）")
    except ValueError:
        print("⚠ 入力が無効です。スキップします。")
        return []
    
    if len(selected_files) < 2:
        print("⚠ 少なくとも2つのファイルを選択してください")
        return []
    
    # 選択されたファイルから期間でソートして最新と最古を返す
    file_dates = []
    for csv_file in selected_files:
        try:
            start_date, end_date = extract_date_range(csv_file.name)
            file_dates.append((end_date, csv_file))
        except ValueError:
            continue
    
    if len(file_dates) < 2:
        print("⚠ 日付範囲を検出できるファイルが2つ以上必要です")
        return []
    
    file_dates.sort(key=lambda x: x[0])
    period_b_file = file_dates[0][1]  # 古い方（前期間）
    period_a_file = file_dates[-1][1]  # 新しい方（後期間）
    
    print(f"\n✓ 選択されたファイル:")
    print(f"  後期間: {period_a_file.name}")
    print(f"  前期間: {period_b_file.name}")
    
    return [period_a_file, period_b_file]


def move_campaigns_files(csv_files: List[Path], totals_dir: Path) -> Tuple[Path, Path]:
    """Campaigns.csvファイルをtotalsフォルダに移動"""
    # ユーザーに選択してもらう
    selected_files = select_campaigns_files(csv_files)
    
    if len(selected_files) != 2:
        raise ValueError("ファイルの選択が完了していません")
    
    period_a_file = selected_files[1]  # 新しい方（後期間）
    period_b_file = selected_files[0]  # 古い方（前期間）
    
    # totalsディレクトリが存在しない場合は作成
    totals_dir.mkdir(parents=True, exist_ok=True)
    
    # ファイルをコピー（元のファイルは残す）
    dest_b = totals_dir / period_b_file.name
    dest_a = totals_dir / period_a_file.name
    
    if not dest_b.exists():
        print(f"\n[移動] {period_b_file.name} → {totals_dir}")
        shutil.copy2(period_b_file, dest_b)
    else:
        print(f"\n[スキップ] {dest_b.name} は既に存在します")
    
    if not dest_a.exists():
        print(f"[移動] {period_a_file.name} → {totals_dir}")
        shutil.copy2(period_a_file, dest_a)
    else:
        print(f"[スキップ] {dest_a.name} は既に存在します")
    
    return dest_a, dest_b


def select_keyword_files(excel_files: List[Path], period_a_dir: Path, period_b_dir: Path):
    """ユーザーにキーワードデータのファイル（Excel/CSV）の選択を求める"""
    # 既存のファイルをチェック（CSVも含める）
    existing_a = set(p.name for p in period_a_dir.iterdir() 
                    if p.is_file() 
                    and not p.name.startswith(".")
                    and p.suffix.lower() in {".xlsx", ".xls", ".csv"})
    existing_b = set(p.name for p in period_b_dir.iterdir() 
                    if p.is_file() 
                    and not p.name.startswith(".")
                    and p.suffix.lower() in {".xlsx", ".xls", ".csv"})
    
    # 既に配置されているファイルを除外
    candidates = [f for f in excel_files if f.name not in existing_a and f.name not in existing_b]
    
    if not candidates:
        print("⚠ 新しく配置できるキーワード別レポートファイルが見つかりませんでした。")
        if existing_a or existing_b:
            print(f"\n既存のキーワード別レポートファイルが見つかりました:")
            if existing_a:
                print(f"  後期間: {len(existing_a)}個")
            if existing_b:
                print(f"  前期間: {len(existing_b)}個")
            print("既存のファイルを使用して分析を続行します。")
        else:
            print("既存のファイルも見つかりませんでした。")
            print("手動でファイルを配置してください。")
        return
    
    print("\n[キーワードデータのファイル（キーワード別レポート）の選択]")
    print("=" * 60)
    print(f"検出されたファイル: {len(candidates)}個\n")
    
    # 「キーワード別レポート」を含むファイルのみを表示（既にフィルタリング済み）
    file_list = []
    
    # キーワード別レポートを処理
    for excel_file in candidates:
        # ファイル名から日付を検出
        date_str = None
        date_match = re.search(r"(\d{4})-(\d{2})-(\d{2})|(\d{8})", excel_file.name)
        if date_match:
            try:
                if date_match.group(1):  # YYYY-MM-DD形式
                    date_str = f"{date_match.group(1)}/{date_match.group(2)}/{date_match.group(3)}"
                else:  # YYYYMMDD形式
                    date_str = date_match.group(4)
                    date_str = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:8]}"
            except:
                pass
        
        if not date_str:
            mtime = datetime.fromtimestamp(excel_file.stat().st_mtime)
            date_str = mtime.strftime("%Y/%m/%d")
        
        # 更新日時を追加（ソート用）
        mtime = excel_file.stat().st_mtime
        file_list.append((date_str, excel_file, mtime))
    
    # 更新日時でソート（新しい順）
    file_list.sort(key=lambda x: x[2], reverse=True)  # x[2]はmtime（更新日時）
    
    # 一覧表示（最大50個まで）
    display_count = min(50, len(file_list))
    print("\n【キーワード別レポート】")
    for i, (date_str, excel_file, mtime) in enumerate(file_list[:display_count], 1):
        size_mb = excel_file.stat().st_size / (1024 * 1024)
        print(f"{i:3d}. ★ [{date_str}] {excel_file.name} ({size_mb:.1f}MB)")
    
    if len(file_list) > 0:
        print(f"\n★ マーク = キーワード別レポート（推奨）")
    
    if len(file_list) > display_count:
        print(f"\n... 他 {len(file_list) - display_count}個のファイル")
    
    print("\n" + "=" * 60)
    print("使用するファイルの番号を入力してください（複数選択可。カンマ区切り）:")
    print("例: 1,3,5 または 1-10（範囲指定）")
    print("Enterキーでスキップ（既存のファイルがあれば使用します）")
    user_input = input("> ").strip()
    
    selected_files = []
    if user_input:
        try:
            for part in user_input.split(","):
                part = part.strip()
                if "-" in part:
                    # 範囲指定
                    start, end = part.split("-")
                    selected_indices = range(int(start.strip()), int(end.strip()) + 1)
                else:
                    selected_indices = [int(part.strip())]
                
                for idx in selected_indices:
                    if 1 <= idx <= display_count:
                        selected_files.append(file_list[idx - 1][1])  # [1]はexcel_file
                    else:
                        print(f"⚠ 番号 {idx} は無効です（1-{display_count}の範囲で指定してください）")
        except ValueError:
            print("⚠ 入力が無効です。スキップします。")
            return
        
        if not selected_files:
            print("\nファイルが選択されませんでした。")
            if existing_a or existing_b:
                print("既存のファイルを使用して分析を続行します。")
            else:
                print("既存のファイルも見つかりませんでした。")
                print("手動でファイルを配置してください。")
            return
        
        if selected_files:
            print(f"\n選択されたファイル: {len(selected_files)}個")
            
            # 後期間と前期間に分類するかユーザーに確認
            print("\nこれらのファイルを後期間と前期間に分類しますか？")
            print("1. 自動分類（日付で判定）")
            print("2. 手動で指定")
            print("3. すべて後期間に入れる")
            print("4. すべて前期間に入れる")
            choice = input("選択 (1-4, Enter=スキップ): ").strip()
            
            period_a_files = []
            period_b_files = []
            
            if choice == "1":
                # 自動分類
                for excel_file in selected_files:
                    date_match = re.search(r"(\d{4})-(\d{2})-(\d{2})|(\d{8})", excel_file.name)
                    if date_match:
                        try:
                            if date_match.group(1):  # YYYY-MM-DD形式
                                file_date = datetime.strptime(f"{date_match.group(1)}{date_match.group(2)}{date_match.group(3)}", "%Y%m%d")
                            else:  # YYYYMMDD形式
                                file_date = datetime.strptime(date_match.group(4), "%Y%m%d")
                            
                            threshold = datetime(2025, 1, 1)
                            if file_date >= threshold:
                                period_a_files.append(excel_file)
                            else:
                                period_b_files.append(excel_file)
                        except ValueError:
                            period_a_files.append(excel_file)
                    else:
                        period_a_files.append(excel_file)
            
            elif choice == "2":
                # 手動で指定
                print("\n後期間に入れるファイルの番号を入力してください:")
                period_a_indices_input = input("> ").strip()
                if period_a_indices_input:
                    period_a_indices = set()
                    for part in period_a_indices_input.split(","):
                        part = part.strip()
                        if "-" in part:
                            start, end = part.split("-")
                            period_a_indices.update(range(int(start.strip()), int(end.strip()) + 1))
                        else:
                            period_a_indices.add(int(part.strip()))
                    
                    for i, excel_file in enumerate(selected_files, 1):
                        if i in period_a_indices:
                            period_a_files.append(excel_file)
                        else:
                            period_b_files.append(excel_file)
                else:
                    period_b_files = selected_files
            
            elif choice == "3":
                period_a_files = selected_files
            
            elif choice == "4":
                period_b_files = selected_files
            else:
                # 選択が無効な場合はスキップ
                print("選択が無効です。スキップします。")
                return
            
            # ファイルをコピー
            if period_a_files:
                for excel_file in period_a_files:
                    dest = period_a_dir / excel_file.name
                    print(f"[移動] {excel_file.name} → {period_a_dir}")
                    shutil.copy2(excel_file, dest)
            
            if period_b_files:
                for excel_file in period_b_files:
                    dest = period_b_dir / excel_file.name
                    print(f"[移動] {excel_file.name} → {period_b_dir}")
                    shutil.copy2(excel_file, dest)


def move_keyword_files(excel_files: List[Path], period_a_dir: Path, period_b_dir: Path):
    """キーワードデータのファイルを適切なフォルダに移動"""
    period_a_dir.mkdir(parents=True, exist_ok=True)
    period_b_dir.mkdir(parents=True, exist_ok=True)
    
    select_keyword_files(excel_files, period_a_dir, period_b_dir)


def main():
    args = parse_args()
    
    # data-sourceが指定された場合、ローカルからdata/にコピー
    if args.data_source:
        print(f"データソース: {args.data_source}")
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)
        
        # サブディレクトリ作成
        (data_dir / "後期間").mkdir(exist_ok=True)
        (data_dir / "前期間").mkdir(exist_ok=True)
        (data_dir / "totals").mkdir(exist_ok=True)
        
        # ファイルをコピー
        for src_file in args.data_source.rglob("*"):
            if src_file.is_file() and src_file.suffix.lower() in {".xlsx", ".xls", ".csv"}:
                if "後期間" in src_file.name or "period_a" in src_file.name:
                    dest = data_dir / "後期間" / src_file.name
                elif "前期間" in src_file.name or "period_b" in src_file.name:
                    dest = data_dir / "前期間" / src_file.name
                elif "Campaigns" in src_file.name:
                    dest = data_dir / "totals" / src_file.name
                else:
                    # 自動判定: ファイル名に日付で分類
                    if re.search(r"\d{8}-\d{8}", src_file.name):
                        dest = data_dir / "totals" / src_file.name
                    else:
                        dest = data_dir / "後期間" / src_file.name  # デフォルト後期間
                shutil.copy2(src_file, dest)
                print(f"コピー: {src_file} -> {dest}")
        
        input_dir = data_dir
    else:
        input_dir = Path("data")
    
    parser = argparse.ArgumentParser(description="アカウント分析くん")
    parser.add_argument(
        "--downloads-dir",
        type=Path,
        default=None,
        help="Downloadsフォルダのパス（デフォルト: ~/Downloads）",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=input_dir,
        help="データディレクトリ（デフォルト: data）",
    )
    parser.add_argument(
        "--skip-move",
        action="store_true",
        help="ファイルの移動をスキップして分析のみ実行",
    )
    
    args = parser.parse_args()
    
    # Downloadsフォルダのパスを決定
    if args.downloads_dir is None:
        downloads_dir = get_downloads_folder()
    else:
        downloads_dir = args.downloads_dir
    
    if not downloads_dir.exists():
        raise FileNotFoundError(f"Downloadsフォルダが見つかりません: {downloads_dir}")
    
    print("=" * 60)
    print("アカウント分析くん")
    print("=" * 60)
    print(f"Downloadsフォルダ: {downloads_dir}")
    print(f"データディレクトリ: {args.input_dir}")
    print()
    
    # ファイルの移動をスキップしない場合のみ実行
    if not args.skip_move:
        print("[ステップ1] Campaigns.csvファイルを検出中...")
        csv_files = find_campaigns_csv_files(downloads_dir)
        print(f"検出されたファイル: {len(csv_files)}個")
        
        totals_dir = args.input_dir / "totals"
        totals_dir.mkdir(parents=True, exist_ok=True)
        
        # 既存のファイルをチェック
        existing_csv = [p for p in totals_dir.iterdir() 
                       if p.is_file() and p.suffix.lower() == ".csv" and "Campaigns" in p.name]
        
        if csv_files:
            try:
                period_a_file, period_b_file = move_campaigns_files(csv_files, totals_dir)
                print(f"\n✓ 後期間ファイル: {period_a_file.name}")
                print(f"✓ 前期間ファイル: {period_b_file.name}")
                print("✓ Campaigns.csvファイルの選択が完了しました")
            except ValueError as e:
                print(f"\n✗ エラー: {e}")
                if existing_csv:
                    print(f"\n既存のCampaigns.csvファイルが見つかりました: {len(existing_csv)}個")
                    for f in existing_csv:
                        print(f"  - {f.name}")
                    print("\n既存のファイルを使用して分析を続行しますか？ (y/n): ", end="")
                    try:
                        choice = input().strip().lower()
                        if choice != 'y':
                            print("処理を中断しました。")
                            return
                    except EOFError:
                        print("y")
                        # 対話型入力ができない場合は続行
                else:
                    print("\n⚠ 使用可能なCampaigns.csvファイルがありません。")
                    print("手動でファイルを data/totals/ に配置してください。")
                    print("続行しますか？ (y/n): ", end="")
                    try:
                        choice = input().strip().lower()
                        if choice != 'y':
                            print("処理を中断しました。")
                            return
                    except EOFError:
                        print("y")
                        # 対話型入力ができない場合は続行
        else:
            print("⚠ DownloadsフォルダにCampaigns.csvファイルが見つかりませんでした。")
            if existing_csv:
                print(f"\n既存のCampaigns.csvファイルが見つかりました: {len(existing_csv)}個")
                for f in existing_csv:
                    print(f"  - {f.name}")
                print("既存のファイルを使用して分析を続行します。")
            else:
                print("⚠ 使用可能なCampaigns.csvファイルがありません。")
                print("手動でファイルを data/totals/ に配置してください。")
                print("続行しますか？ (y/n): ", end="")
                try:
                    choice = input().strip().lower()
                    if choice != 'y':
                        print("処理を中断しました。")
                        return
                except EOFError:
                    print("y")
                    # 対話型入力ができない場合は続行
        print()
        
        print("[ステップ2] キーワードデータのファイル（キーワード別レポート）を検出中...")
        excel_files = find_keyword_excel_files(downloads_dir)
        print(f"検出されたファイル: {len(excel_files)}個")
        
        period_a_dir = args.input_dir / "後期間"
        period_b_dir = args.input_dir / "前期間"
        period_a_dir.mkdir(parents=True, exist_ok=True)
        period_b_dir.mkdir(parents=True, exist_ok=True)
        
        # 既存のファイルをチェック（CSVも含める）
        existing_excel_a = [p for p in period_a_dir.iterdir() 
                           if p.is_file() 
                           and p.suffix.lower() in {".xlsx", ".xls", ".csv"}
                           and not p.name.startswith(".")]
        existing_excel_b = [p for p in period_b_dir.iterdir() 
                           if p.is_file() 
                           and p.suffix.lower() in {".xlsx", ".xls", ".csv"}
                           and not p.name.startswith(".")]
        
        if excel_files:
            move_keyword_files(excel_files, period_a_dir, period_b_dir)
        else:
            print("⚠ Downloadsフォルダにキーワード別レポートファイルが見つかりませんでした。")
            if existing_excel_a or existing_excel_b:
                print(f"\n既存のキーワード別レポートファイルが見つかりました:")
                if existing_excel_a:
                    print(f"  後期間: {len(existing_excel_a)}個")
                if existing_excel_b:
                    print(f"  前期間: {len(existing_excel_b)}個")
                print("既存のファイルを使用して分析を続行します。")
            else:
                print("⚠ 使用可能なキーワード別レポートファイルがありません。")
                print("手動でファイルを data/後期間/ と data/前期間/ に配置してください。")
                print("続行しますか？ (y/n): ", end="")
                try:
                    choice = input().strip().lower()
                    if choice != 'y':
                        print("処理を中断しました。")
                        return
                except EOFError:
                    print("y")
                    # 対話型入力ができない場合は続行
        print()
    
    # 必要なファイルが揃ったことを確認してから分析を開始
    totals_dir = args.input_dir / "totals"
    period_a_dir = args.input_dir / "後期間"
    period_b_dir = args.input_dir / "前期間"
    
    existing_csv = [p for p in totals_dir.iterdir() 
                   if p.is_file() and p.suffix.lower() == ".csv" and "Campaigns" in p.name] if totals_dir.exists() else []
    existing_excel_a = [p for p in period_a_dir.iterdir() 
                       if p.is_file() 
                       and p.suffix.lower() in {".xlsx", ".xls", ".csv"}
                       and not p.name.startswith(".")] if period_a_dir.exists() else []
    existing_excel_b = [p for p in period_b_dir.iterdir() 
                       if p.is_file() 
                       and p.suffix.lower() in {".xlsx", ".xls", ".csv"}
                       and not p.name.startswith(".")] if period_b_dir.exists() else []
    
    print("=" * 60)
    print("[ファイル準備状況の確認]")
    print("=" * 60)
    if existing_csv:
        print(f"✓ Campaigns.csvファイル: {len(existing_csv)}個")
        for f in existing_csv:
            print(f"  - {f.name}")
    else:
        print("⚠ Campaigns.csvファイル: なし")
    
    if existing_excel_a:
        print(f"✓ 後期間ファイル: {len(existing_excel_a)}個")
    else:
        print("⚠ 後期間ファイル: なし")
    
    if existing_excel_b:
        print(f"✓ 前期間ファイル: {len(existing_excel_b)}個")
    else:
        print("⚠ 前期間ファイル: なし")
    
    print()
    
    if existing_csv and (existing_excel_a or existing_excel_b):
        print("必要なファイルが揃いました。分析を開始します。")
    elif not existing_csv:
        print("⚠ Campaigns.csvファイルが不足しています。分析を続行しますが、エラーが発生する可能性があります。")
    elif not existing_excel_a and not existing_excel_b:
        print("⚠ キーワード別レポートファイルが不足しています。分析を続行しますが、エラーが発生する可能性があります。")
    else:
        print("分析を開始します。")
    
    print()
    
    print("[ステップ3] 分析を実行中...")
    print()
    
    # run_grouping.pyのmain関数を実行
    # run_grouping.py は run.py の parse_args を使うため、--input-dir と --output-dir を指定する
    import sys
    original_argv = sys.argv.copy()
    sys.argv = [sys.argv[0], "--input-dir", str(args.input_dir), "--output-dir", str(args.input_dir / "output")]
    
    try:
        run_grouping.main()
    finally:
        sys.argv = original_argv
    
    print()
    print("=" * 60)
    print("処理が完了しました！")
    print("=" * 60)


if __name__ == "__main__":
    main()
