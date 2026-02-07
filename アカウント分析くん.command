#!/bin/bash
cd "$(dirname "$0")"

# Pythonのパスを確認
if ! command -v python3 &> /dev/null; then
    echo "エラー: Python3が見つかりません。"
    echo "Python3をインストールしてください。"
    read -p "Enterキーを押して終了してください..."
    exit 1
fi

# 必要なライブラリがインストールされているか確認
echo "環境を確認しています..."
python3 << 'PYTHON_EOF'
import sys
import subprocess

required_packages = {
    'pandas': 'pandas',
    'openpyxl': 'openpyxl',
    'xlsxwriter': 'xlsxwriter'
}

missing_packages = []
for module_name, package_name in required_packages.items():
    try:
        __import__(module_name)
    except ImportError:
        missing_packages.append(package_name)

if missing_packages:
    print(f"以下のライブラリが見つかりません: {', '.join(missing_packages)}")
    print("インストールを開始します...")
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'])
    print("インストールが完了しました。")
else:
    print("必要なライブラリはすべてインストールされています。")
PYTHON_EOF

if [ $? -ne 0 ]; then
    echo ""
    echo "セットアップ中にエラーが発生しました。"
    read -p "Enterキーを押して終了してください..."
    exit 1
fi

echo ""
echo "処理を開始します..."
echo ""

# アカウント分析くんを実行
python3 account_analyzer.py

# エラーが発生した場合は一時停止
if [ $? -ne 0 ]; then
    echo ""
    echo "エラーが発生しました。"
    read -p "Enterキーを押して終了してください..."
    exit 1
fi

echo ""
echo "処理が完了しました。"
read -p "Enterキーを押して終了してください..."
