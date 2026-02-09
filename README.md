# Wayback Gap Detector

ローカルのURLリストとWayback Machine（CDX API）のデータを突き合わせ、**アーカイブが存在しないURL**のみを抽出するツール。

## 受け入れ条件

### 必須機能
- ✅ CDX APIからアーカイブデータを取得（collapse=urlkey で最適化）
- ✅ ローカルのURLリスト（`urls.txt`）との突き合わせ
- ✅ 未アーカイブURLの検出と出力
- ✅ URLの正規化による誤検知防止
- ✅ キャッシュ機能（JSONファイル）
- ✅ コマンドライン引数によるカスタマイズ

### 正規化仕様
- ✅ プロトコル統一（`http`/`https` を同一視可能、デフォルト有効）
- ✅ ホスト名の小文字化
- ✅ デフォルトポート削除（`:80`, `:443`）
- ✅ パス末尾のスラッシュ削除
- ✅ クエリパラメータのソート（オプション、デフォルト無効）
- ✅ フラグメント削除
- ✅ HTML実体参照のデコード

### 出力
- ✅ 未アーカイブURLを `not_archived.txt` に出力
- ✅ コンソールに統計情報を表示（CDX取得件数、調査対象URL数、未アーカイブ件数）

## 必要要件

- Python 3.7以上
- `requests` ライブラリ

## インストール

```bash
# 依存ライブラリのインストール
pip install requests
```

## 実行手順

### 1. 入力ファイルの準備

調査対象のURLリストを `urls.txt` に記載します（1行1URL）。

**urls.txt の例:**
```
https://example.com/aiueo/page1
https://example.com/aiueo/page2
https://example.com/aiueo/page3
http://example.com/aiueo/page4
```

### 2. プログラムの実行

基本的な使用方法：

```bash
python wbgap.py "https://example.com/aiueo/*"
```

**注意:** 第1引数はワイルドカード付きURLで、検索対象のパスを `*` で指定します。

### 3. 結果の確認

- `not_archived.txt`: アーカイブされていないURLのリスト
- `archived_cdx.json`: CDX APIのキャッシュデータ（次回以降の高速化）

## コマンドライン引数

### 必須引数

- **第1引数**: CDX検索対象のワイルドカードURL
  ```bash
  python wbgap.py "https://example.com/path/*"
  ```

### オプション引数

| オプション | 説明 | デフォルト |
|-----------|------|-----------|
| `--ignore-protocol` | http と https を同一視する | 有効 |
| `--no-ignore-protocol` | http と https を区別する | - |
| `--sort-query` | クエリパラメータをソートする | 無効 |
| `--no-sort-query` | クエリパラメータの順序を維持する | - |
| `--input <filepath>` | 入力URLファイルのパス | `urls.txt` |
| `--output <filepath>` | 出力ファイルのパス | `not_archived.txt` |
| `--cache <filepath>` | キャッシュファイルのパス | `archived_cdx.json` |

## 使用例

### 例1: デフォルト設定で実行

```bash
python wbgap.py "https://example.com/blog/*"
```

- プロトコル無視: 有効
- クエリソート: 無効
- 入力: `urls.txt`
- 出力: `not_archived.txt`

### 例2: クエリパラメータをソートして実行

```bash
python wbgap.py "https://example.com/api/*" --sort-query
```

クエリパラメータの順序が異なるURLを同一視します。

### 例3: プロトコルを区別して実行

```bash
python wbgap.py "https://example.com/secure/*" --no-ignore-protocol
```

`http://` と `https://` を別のURLとして扱います。

### 例4: カスタムファイルパスで実行

```bash
python wbgap.py "https://example.com/data/*" --input my_urls.txt --output missing.txt --cache cache.json
```

### 例5: すべてのオプションを指定

```bash
python wbgap.py "https://example.com/content/*" \
  --sort-query \
  --no-ignore-protocol \
  --input urls_list.txt \
  --output not_found.txt \
  --cache cdx_cache.json
```

## 単体テストの実行

### テストファイル: `test_wbgap.py`

単体テストを実行して、正規化ロジックや検出機能が正しく動作するか確認できます。

```bash
python test_wbgap.py
```

### テスト項目

テストは以下をカバーしています：

#### 1. URL正規化テスト (`TestNormalizeUrl`)
- 基本的な正規化
- HTML実体参照のデコード
- プロトコル無視（有効/無効）
- ホスト名の小文字化
- デフォルトポートの削除
- 非デフォルトポートの保持
- 末尾スラッシュの削除
- ルートパスの保持
- クエリパラメータのソート（有効/無効）
- 空クエリの削除
- フラグメントの削除
- 複雑なURLの正規化

#### 2. アーカイブURL抽出テスト (`TestExtractArchivedUrls`)
- 基本的な抽出
- 空データの処理
- ヘッダーのみのデータ処理

#### 3. 未アーカイブURL検出テスト (`TestDetectNotArchived`)
- すべてアーカイブ済みの場合
- 一部未アーカイブの場合
- すべて未アーカイブの場合
- プロトコル無視での検出

#### 4. 統合テスト (`TestIntegration`)
- エンドツーエンドのワークフロー

### 期待される出力

```
test_basic_normalization (__main__.TestNormalizeUrl) ... ok
test_html_unescape (__main__.TestNormalizeUrl) ... ok
test_protocol_ignore_true (__main__.TestNormalizeUrl) ... ok
...
======================================================================
テスト結果サマリー
======================================================================
実行テスト数: 23
成功: 23
失敗: 0
エラー: 0
======================================================================
```

## 検証内容

### 何を検証したか

1. **URL正規化の正確性**
   - プロトコル、ホスト名、ポート、パス、クエリ、フラグメントの各要素が仕様通りに正規化されることを確認
   - 設定フラグ（`ignore_protocol`, `sort_query`）による動作の切り替えを確認

2. **CDXデータの処理**
   - JSONレスポンスからのURL抽出
   - ヘッダー行の動的解析
   - エッジケース（空データ、ヘッダーのみ）の処理

3. **未アーカイブURLの検出精度**
   - 正規化後の比較による誤検知防止
   - 元のURL表記の保持

4. **エンドツーエンドの動作**
   - 入力ファイル → CDXデータ取得 → 正規化 → 比較 → 出力までの一連の流れ

### どのように検証したか

- **ユニットテスト**: 個別の関数ごとに独立してテスト
- **統合テスト**: 複数の関数を組み合わせた動作をテスト
- **エッジケースのカバー**: 空データ、特殊文字、境界値などを網羅

## トラブルシューティング

### Q1: `urls.txt` が見つからないエラー

**エラーメッセージ:**
```
エラー: 入力ファイルが見つかりません: urls.txt
```

**解決策:**
- `urls.txt` をスクリプトと同じディレクトリに配置する
- または `--input` オプションで正しいパスを指定する

### Q2: CDX APIがタイムアウトする

**症状:**
ネットワークエラーでプログラムが終了する

**解決策:**
- インターネット接続を確認する
- しばらく待ってから再実行する
- キャッシュファイルが存在する場合は、それを利用する

### Q3: キャッシュを更新したい

**解決策:**
キャッシュファイル（デフォルト: `archived_cdx.json`）を削除してから再実行する

```bash
rm archived_cdx.json
python wbgap.py "https://example.com/path/*"
```

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。

## 作成者

Wayback Gap Detector開発チーム

## バージョン履歴

- **v1.0** (2026-02-09): 初回リリース
  - 基本機能実装
  - 単体テスト完備
  - README作成
