#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Wayback Gap Detector
ローカルのURLリストとWayback Machine CDX APIデータを突き合わせ、
アーカイブされていないURLを検出するツール
"""

import argparse
import html
import json
import os
import sys
from typing import Set, List, Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import requests


# ========================================
# デフォルト設定
# ========================================
IGNORE_PROTOCOL = True
SORT_QUERY_PARAMS = False
TARGET_URL_FILE = "urls.txt"
OUTPUT_FILE = "not_archived.txt"
CACHE_FILE = "archived_cdx.json"
ARCHIVED_FILE = "archived.txt"

USER_AGENT = "Wayback-Gap-Detector/1.0 (https://github.com/c3974/wayback-gap-detector)"


# ========================================
# URL正規化関数
# ========================================
def normalize_url(url: str, ignore_protocol: bool = IGNORE_PROTOCOL,
                  sort_query: bool = SORT_QUERY_PARAMS) -> str:
    """
    URLを正規化して比較可能な形式に変換する
    
    Args:
        url: 正規化対象のURL
        ignore_protocol: プロトコル（http/https）を無視するか
        sort_query: クエリパラメータをソートするか
    
    Returns:
        正規化されたURL文字列
    """
    # 1. 前後の空白削除
    url = url.strip()
    
    # 2. HTML実体参照のデコード
    url = html.unescape(url)
    
    # 3. URLをパース
    parsed = urlparse(url)
    
    # 4. スキームの正規化
    original_scheme = parsed.scheme.lower()
    scheme = original_scheme
    if ignore_protocol and scheme in ('http', 'https'):
        scheme = 'http'
    
    # 5. ホスト名の正規化（小文字化）
    netloc = parsed.hostname or ''
    netloc = netloc.lower()
    
    # 6. ポート番号の処理
    port = parsed.port
    if port:
        # デフォルトポートは削除（元のスキームと正規化後のスキームの両方をチェック）
        is_default_port = (
            (original_scheme == 'http' and port == 80) or
            (original_scheme == 'https' and port == 443) or
            (scheme == 'http' and port == 80) or
            (scheme == 'https' and port == 443)
        )
        if not is_default_port:
            netloc = f"{netloc}:{port}"
    
    # 7. パスの正規化
    path = parsed.path
    # 末尾のスラッシュを削除（ルートパス "/" は除く）
    if path and path != '/' and path.endswith('/'):
        path = path.rstrip('/')
    
    # 8. クエリパラメータの処理
    query = parsed.query
    if query:
        if sort_query:
            # クエリパラメータをソート
            params = parse_qs(query, keep_blank_values=True)
            sorted_params = sorted(params.items())
            query = urlencode(sorted_params, doseq=True)
        # else: 元の順序を維持
    else:
        query = ''
    
    # 9. フラグメントは削除（常に空文字列）
    fragment = ''
    
    # 10. 正規化されたURLを再構築
    normalized = urlunparse((scheme, netloc, path, '', query, fragment))
    
    return normalized


# ========================================
# CDX API関連
# ========================================
def fetch_cdx_data(target_url: str, cache_file: str) -> List:
    """
    CDX APIからデータを取得、またはキャッシュから読み込む
    
    Args:
        target_url: 検索対象のワイルドカードURL
        cache_file: キャッシュファイルのパス
    
    Returns:
        CDX APIのJSONレスポンス（リスト形式）
    """

    if os.path.exists(cache_file):
        print(f"キャッシュファイルを読み込み中: {cache_file}")
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f"キャッシュから {len(data)} 件のレコードを読み込みました")
            return data
        except Exception as e:
            print(f"キャッシュ読み込みエラー: {e}")
            print("CDX APIから再取得します...")
    
    print(f"CDX APIからデータを取得中: {target_url}")
    api_url = "https://web.archive.org/cdx/search/cdx"
    
    params = {
        'url': target_url,
        'output': 'json',
        'filter': 'statuscode:200',
        'collapse': 'urlkey'
    }
    
    headers = {
        'User-Agent': USER_AGENT
    }
    
    try:
        response = requests.get(api_url, params=params, headers=headers, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"CDX APIから {len(data)} 件のレコードを取得しました")
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"CDX API取得エラー: {e}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"JSONパースエラー: {e}", file=sys.stderr)
        sys.exit(1)


def extract_archived_urls(cdx_data: List, ignore_protocol: bool,
                          sort_query: bool) -> Set[str]:
    """
    CDXデータから正規化されたURLの集合を抽出
    
    Args:
        cdx_data: CDX APIのレスポンスデータ
        ignore_protocol: プロトコル無視フラグ
        sort_query: クエリソートフラグ
    
    Returns:
        正規化されたURLの集合
    """
    if not cdx_data or len(cdx_data) == 0:
        return set()
    
    header = cdx_data[0]
    try:
        original_idx = header.index('original')
    except (ValueError, AttributeError):
        print("警告: 'original'カラムが見つかりません。デフォルトインデックス2を使用します")
        original_idx = 2
    
    archived_urls = set()
    
    for row in cdx_data[1:]:
        if len(row) > original_idx:
            original_url = row[original_idx]
            normalized = normalize_url(original_url, ignore_protocol, sort_query)
            archived_urls.add(normalized)
    
    return archived_urls


# ========================================
# メイン処理
# ========================================
def detect_not_archived(input_file: str, archived_urls: Set[str],
                        ignore_protocol: bool, sort_query: bool) -> List[str]:
    """
    入力ファイルから未アーカイブURLを検出
    
    Args:
        input_file: 入力URLファイルのパス
        archived_urls: アーカイブ済みURLの集合（正規化済み）
        ignore_protocol: プロトコル無視フラグ
        sort_query: クエリソートフラグ
    
    Returns:
        未アーカイブURLのリスト（元の表記）
    """
    if not os.path.exists(input_file):
        print(f"エラー: 入力ファイルが見つかりません: {input_file}", file=sys.stderr)
        sys.exit(1)
    
    not_archived = []
    
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            original_url = line.strip()
            if not original_url:
                continue
            
            normalized = normalize_url(original_url, ignore_protocol, sort_query)
            
            if normalized not in archived_urls:
                not_archived.append(original_url)
    
    return not_archived


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(
        description='Wayback Gap Detector - アーカイブされていないURLを検出',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
使用例:
  python wbgap.py "https://example.com/aiueo/*"
  python wbgap.py "https://example.com/aiueo/*" --sort-query --output result.txt
        '''
    )
    
    parser.add_argument(
        'target_url',
        help='CDX検索対象のワイルドカードURL（例: https://example.com/path/*）'
    )
    
    parser.add_argument(
        '--ignore-protocol',
        dest='ignore_protocol',
        action='store_true',
        default=IGNORE_PROTOCOL,
        help='http と https を同一視する（デフォルト: 有効）'
    )
    parser.add_argument(
        '--no-ignore-protocol',
        dest='ignore_protocol',
        action='store_false',
        help='http と https を区別する'
    )
    
    parser.add_argument(
        '--sort-query',
        dest='sort_query',
        action='store_true',
        default=SORT_QUERY_PARAMS,
        help='クエリパラメータをソートする（デフォルト: 無効）'
    )
    parser.add_argument(
        '--no-sort-query',
        dest='sort_query',
        action='store_false',
        help='クエリパラメータの順序を維持する'
    )
    
    parser.add_argument(
        '--input',
        dest='input_file',
        default=TARGET_URL_FILE,
        help=f'入力URLファイルのパス（デフォルト: {TARGET_URL_FILE}）'
    )
    
    parser.add_argument(
        '--output',
        dest='output_file',
        default=OUTPUT_FILE,
        help=f'出力ファイルのパス（デフォルト: {OUTPUT_FILE}）'
    )
    
    parser.add_argument(
        '--cache',
        dest='cache_file',
        default=CACHE_FILE,
        help=f'キャッシュファイルのパス（デフォルト: {CACHE_FILE}）'
    )

    parser.add_argument(
        '--output-archived',
        dest='output_archived',
        nargs='?',
        const=ARCHIVED_FILE,
        default=None,
        help=f'アーカイブ済みURLの出力ファイルパス（デフォルト: {ARCHIVED_FILE}）'
    )
    
    args = parser.parse_args()
    
    cdx_data = fetch_cdx_data(args.target_url, args.cache_file)
    
    archived_urls = extract_archived_urls(
        cdx_data,
        args.ignore_protocol,
        args.sort_query
    )
    cdx_raw_count = len(cdx_data) - 1
    print(f"\nCDX取得件数: {cdx_raw_count}")
    
    not_archived = detect_not_archived(
        args.input_file,
        archived_urls,
        args.ignore_protocol,
        args.sort_query
    )

    confirmed_archived_urls = []
    not_archived_set = set(not_archived)
    
    with open(args.input_file, 'r', encoding='utf-8') as f:
        input_url_count = 0
        for line in f:
            url = line.strip()
            if not url:
                continue
            input_url_count += 1
            
            if args.output_archived and url not in not_archived_set:
                confirmed_archived_urls.append(url)
    
    with open(args.output_file, 'w', encoding='utf-8') as f:
        for url in not_archived:
            f.write(url + '\n')
    
    print(f"調査対象URL数: {input_url_count}")
    print(f"未アーカイブ件数: {len(not_archived)}")
    print(f"\n結果を {args.output_file} に出力しました")

    if args.output_archived:
        with open(args.output_archived, 'w', encoding='utf-8') as f:
            for url in confirmed_archived_urls:
                f.write(url + '\n')
        print(f"アーカイブ済みURL（入力ファイル内）を {args.output_archived} に出力しました: {len(confirmed_archived_urls)} 件")


if __name__ == '__main__':
    main()
