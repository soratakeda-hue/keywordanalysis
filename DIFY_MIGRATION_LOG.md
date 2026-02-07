# Dify 移行メモ（keywordanalysis）

## 目的
- 既存のPythonツール（keywordanalysis）をDifyアプリ化し、メンバーが4ファイルをアップロードするだけでExcelレポートを生成できるようにする。
- 合算ルールが未入力の場合は `run.py`、入力がある場合は `run_grouping.py` 相当の処理を実行する。

## 前提
- Difyはセルフホスト環境の可能性が高い（独自ドメイン）
- DifyのCode実行ノードで `pandas` / `openpyxl` が未導入（確認済み: `ModuleNotFoundError: No module named 'pandas'`）

## 入力仕様（想定）
- 後期間 Excel（キーワードレポート）
- 前期間 Excel（キーワードレポート）
- 後期間 Campaigns.csv
- 前期間 Campaigns.csv
- 合算ルール（任意）
  - 1行形式: `グループ名,キーワード1,キーワード2` を改行区切り

## 出力仕様（想定）
- Excelファイルを生成し、Difyの出力としてダウンロード

## 既存ツールの要点
- `run.py`: 合算なしの通常処理
- `run_grouping.py`: 合算ルールありの場合の処理（キャンペーン名をグルーピング）
- 依存ライブラリ: `pandas`, `openpyxl`, `xlsxwriter`

## 課題
- Code実行ノードに `pandas` / `openpyxl` が未インストール
- 外部APIはセキュリティ上ハードルが高い

## 次アクション（実行プラン）
1. **管理者にライブラリ追加を依頼**
   - `pandas>=2.0.0`
   - `openpyxl>=3.1.0`
   - `xlsxwriter>=3.1.0`
2. **Difyワークフロー作成**
   - 入力フォーム: 4ファイル + 合算ルール（任意）
   - 条件分岐: 合算ルールの有無で `run.py` / `run_grouping.py` 相当処理を切替
3. **Code実行ノードでPythonロジック実装**
   - 既存の `run.py` / `run_grouping.py` 相当の処理を移植
   - Excelを `BytesIO` で生成し、ダウンロード出力
4. **DSLエクスポート**
   - DifyからDSL（YAML）をエクスポートし、アプリ定義を共有可能にする

## メモ
- Difyクラウド版ではコンテナ実行不可のため、セルフホストでライブラリ追加が必須。
- ファイル名は流動的でも、Dify側の入力欄を固定すれば問題なし。
