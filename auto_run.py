#!/usr/bin/env python3
"""
アカウント分析くん
Downloadsフォルダからファイルを検出し、適切なフォルダに移動してから分析を実行します。
"""
import argparse
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import run  # run.pyのmain関数を実行するため


def get_downloads_folder() -> Path:
    """Downloadsフォルダのパスを取得"""
    home = Path.home()
    downloads = home / "Downloads"
    return downloads


def find_campaigns_csv_files(downloads_dir: Path) -> List[Path]:
    """DownloadsフォルダからCampaigns.csvファイルを検出"""
    csv_files = [
        p for p in downloads_dir.iterdir()
        if p.is_file()
        and p.suffix.lower() == ".csv"
        and "Campaigns" in p.name
        and re.search(r"\d{8}-\d{8}", p.name)  # 日付範囲を含む
    ]
    return csv_files


def find_keyword_excel_files(downloads_dir: Path) -> List[Path]:
    """DownloadsフォルダからキーワードデータのExcelファイルを検出"""
    all_excel_files = [
        p for p in downloads_dir.iterdir()
        if p.is_file()
        and p.suffix.lower() in {".xlsx", ".xls"}
        and not p.name.startswith(".")  # .DS_Storeなどを除外
        and not p.name.startswith("~")  # 一時ファイルを除外
    ]
    
    # 「キーワード別レポート」を含むファイルを優先
    keyword_files = [f for f in all_excel_files if "キーワード別レポート" in f.name]
    other_files = [f for f in all_excel_files if "キーワード別レポート" not in f.name]
    
    # キーワード別レポートを先に、その他を後に
    return keyword_files + other_files


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
    """ユーザーにCampaigns.csvファイルの選択を求める"""
    if len(csv_files) < 2:
        print("⚠ Campaigns.csvファイルが2つ以上必要です（現在: {len(csv_files)}個）")
        return []
    
    # アカウントごとにグループ化
    grouped = group_campaigns_by_account(csv_files)
    
    print("\n[Campaigns.csvファイルの選択]")
    print("=" * 60)
    print("検出されたCampaigns.csvファイル（アカウント別）:\n")
    
    account_index = 0
    account_files_map = {}
    all_files_list = []
    
    for account_name, files in sorted(grouped.items()):
        account_index += 1
        print(f"{account_index}. アカウント: {account_name}")
        print(f"   ファイル数: {len(files)}個")
        for f in sorted(files, key=lambda x: extract_date_range(x.name)[1], reverse=True):
            start_date, end_date = extract_date_range(f.name)
            print(f"     - {f.name}")
            print(f"       期間: {start_date.strftime('%Y/%m/%d')} ～ {end_date.strftime('%Y/%m/%d')}")
        print()
        account_files_map[account_index] = files
        all_files_list.extend(files)
    
    # ユーザーにアカウントを選択してもらう
    print("=" * 60)
    print("使用するアカウントの番号を入力してください（複数選択可。カンマ区切り）:")
    print("例: 1 または 1,2")
    user_input = input("> ").strip()
    
    selected_files = []
    if not user_input:
        print("⚠ ファイルが選択されませんでした。スキップします。")
        return []
    
    try:
        selected_indices = [int(x.strip()) for x in user_input.split(",")]
        for idx in selected_indices:
            if idx in account_files_map:
                selected_files.extend(account_files_map[idx])
            else:
                print(f"⚠ 番号 {idx} は無効です（1-{account_index}の範囲で指定してください）")
    except ValueError:
        print("⚠ 入力が無効です。数値で入力してください。")
        return []
    
    if len(selected_files) < 2:
        print("⚠ 少なくとも2つのファイルを選択してください")
        return []
    
    # 選択されたアカウントのファイルを確認
    # 複数のアカウントを選択した場合、または1つのアカウントでも複数ファイルがある場合は選択を求める
    need_file_selection = False
    selected_accounts_files = {}
    account_names_list = sorted(grouped.keys())
    
    for idx in selected_indices:
        if idx in account_files_map:
            files = account_files_map[idx]
            # アカウント名を取得
            account_name = account_names_list[idx - 1] if 1 <= idx <= len(account_names_list) else f"アカウント{idx}"
            
            if len(files) > 1:
                # 複数ファイルがある場合は必ず選択を求める
                need_file_selection = True
                selected_accounts_files[account_name] = files
            elif len(selected_indices) > 1:
                # 複数アカウントを選択した場合も、ファイル選択を求める
                need_file_selection = True
                selected_accounts_files[account_name] = files
    
    # 複数のファイルがある場合、または複数アカウントを選択した場合は、その中から選択させる
    if need_file_selection:
        print("\n" + "=" * 60)
        print("選択したアカウントのファイル一覧")
        print("使用するファイルを選択してください（2つ以上）:\n")
        
        all_candidate_files = []
        file_index_map = {}
        file_index = 0
        account_names_list = sorted(grouped.keys())
        
        # 選択されたすべてのアカウントのファイルを表示
        for idx in selected_indices:
            if idx in account_files_map:
                files = account_files_map[idx]
                account_name = account_names_list[idx - 1] if 1 <= idx <= len(account_names_list) else f"アカウント{idx}"
                print(f"【{account_name}】")
                for f in sorted(files, key=lambda x: extract_date_range(x.name)[1], reverse=True):
                    file_index += 1
                    start_date, end_date = extract_date_range(f.name)
                    print(f"  {file_index}. {f.name}")
                    print(f"     期間: {start_date.strftime('%Y/%m/%d')} ～ {end_date.strftime('%Y/%m/%d')}")
                    all_candidate_files.append(f)
                    file_index_map[file_index] = f
                print()
        
        print("=" * 60)
        print("使用するファイルの番号を入力してください（2つ以上選択、カンマ区切り）:")
        print("例: 1,3 または 1-3（範囲指定）")
        file_selection_input = input("> ").strip()
        
        if not file_selection_input:
            print("⚠ ファイルが選択されませんでした。スキップします。")
            return []
        
        selected_file_indices = []
        try:
            for part in file_selection_input.split(","):
                part = part.strip()
                if "-" in part:
                    # 範囲指定
                    start, end = part.split("-")
                    selected_file_indices.extend(range(int(start.strip()), int(end.strip()) + 1))
                else:
                    selected_file_indices.append(int(part.strip()))
        except ValueError:
            print("⚠ 入力が無効です。数値で入力してください。")
            return []
        
        final_selected_files = []
        for idx in selected_file_indices:
            if idx in file_index_map:
                final_selected_files.append(file_index_map[idx])
            else:
                print(f"⚠ 番号 {idx} は無効です（1-{file_index}の範囲で指定してください）")
        
        if len(final_selected_files) < 2:
            print("⚠ 少なくとも2つのファイルを選択してください")
            return []
        
        selected_files = final_selected_files
    
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
    """ユーザーにキーワードデータのExcelファイルの選択を求める"""
    # 既存のファイルをチェック
    existing_a = set(p.name for p in period_a_dir.iterdir() if p.is_file() and not p.name.startswith("."))
    existing_b = set(p.name for p in period_b_dir.iterdir() if p.is_file() and not p.name.startswith("."))
    
    # 既に配置されているファイルを除外
    candidates = [f for f in excel_files if f.name not in existing_a and f.name not in existing_b]
    
    if not candidates:
        print("⚠ 新しく配置できるExcelファイルが見つかりませんでした。")
        if existing_a or existing_b:
            print(f"\n既存のExcelファイルが見つかりました:")
            if existing_a:
                print(f"  後期間: {len(existing_a)}個")
            if existing_b:
                print(f"  前期間: {len(existing_b)}個")
            print("既存のファイルを使用して分析を続行します。")
        else:
            print("既存のファイルも見つかりませんでした。")
            print("手動でファイルを配置してください。")
        return
    
    print("\n[キーワードデータのExcelファイルの選択]")
    print("=" * 60)
    print(f"検出されたExcelファイル: {len(candidates)}個\n")
    
    # 「キーワード別レポート」を含むファイルを優先
    keyword_files = [f for f in candidates if "キーワード別レポート" in f.name]
    other_files = [f for f in candidates if "キーワード別レポート" not in f.name]
    
    # ファイルを日付順でソートして表示
    file_list = []
    
    # キーワード別レポートを先に処理
    for excel_file in keyword_files:
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
        
        file_list.append((date_str, excel_file, True))  # True = キーワード別レポート
    
    # その他のファイルを処理
    for excel_file in other_files:
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
        
        file_list.append((date_str, excel_file, False))  # False = その他
    
    # キーワード別レポートを先に、日付でソート（新しい順）
    file_list.sort(key=lambda x: (not x[2], x[0]), reverse=True)  # キーワード別レポート優先、その後日付順
    
    # 一覧表示（最大50個まで）
    display_count = min(50, len(file_list))
    print("\n【キーワード別レポート】")
    keyword_count = 0
    for i, (date_str, excel_file, is_keyword) in enumerate(file_list[:display_count], 1):
        size_mb = excel_file.stat().st_size / (1024 * 1024)
        marker = "★" if is_keyword else " "
        print(f"{i:3d}. {marker} [{date_str}] {excel_file.name} ({size_mb:.1f}MB)")
        if is_keyword:
            keyword_count += 1
    
    if keyword_count > 0:
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
    """キーワードデータのExcelファイルを適切なフォルダに移動"""
    period_a_dir.mkdir(parents=True, exist_ok=True)
    period_b_dir.mkdir(parents=True, exist_ok=True)
    
    select_keyword_files(excel_files, period_a_dir, period_b_dir)


def main():
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
        default=Path("data"),
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
        
        print("[ステップ2] キーワードデータのExcelファイルを検出中...")
        excel_files = find_keyword_excel_files(downloads_dir)
        print(f"検出されたファイル: {len(excel_files)}個")
        
        period_a_dir = args.input_dir / "後期間"
        period_b_dir = args.input_dir / "前期間"
        period_a_dir.mkdir(parents=True, exist_ok=True)
        period_b_dir.mkdir(parents=True, exist_ok=True)
        
        # 既存のファイルをチェック
        existing_excel_a = [p for p in period_a_dir.iterdir() 
                           if p.is_file() and p.suffix.lower() in {".xlsx", ".xls"} 
                           and not p.name.startswith(".")]
        existing_excel_b = [p for p in period_b_dir.iterdir() 
                           if p.is_file() and p.suffix.lower() in {".xlsx", ".xls"} 
                           and not p.name.startswith(".")]
        
        if excel_files:
            move_keyword_files(excel_files, period_a_dir, period_b_dir)
        else:
            print("⚠ DownloadsフォルダにExcelファイルが見つかりませんでした。")
            if existing_excel_a or existing_excel_b:
                print(f"\n既存のExcelファイルが見つかりました:")
                if existing_excel_a:
                    print(f"  後期間: {len(existing_excel_a)}個")
                if existing_excel_b:
                    print(f"  前期間: {len(existing_excel_b)}個")
                print("既存のファイルを使用して分析を続行します。")
            else:
                print("⚠ 使用可能なExcelファイルがありません。")
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
    
    print("[ステップ3] 分析を実行中...")
    print()
    
    # run.pyのmain関数を実行
    # run.pyのparse_argsを上書きして、--input-dirと--output-dirを指定
    import sys
    original_argv = sys.argv.copy()
    sys.argv = [sys.argv[0], "--input-dir", str(args.input_dir), "--output-dir", str(args.input_dir / "output")]
    
    try:
        run.main()
    finally:
        sys.argv = original_argv
    
    print()
    print("=" * 60)
    print("処理が完了しました！")
    print("=" * 60)


if __name__ == "__main__":
    main()
