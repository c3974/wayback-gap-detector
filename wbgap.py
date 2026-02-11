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
import logging
import os
import sys
from pathlib import Path
from typing import Set, List, Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from exceptions import CDXAPIError, InputFileError

# ロガー設定
logger = logging.getLogger(__name__)


# ========================================
# デフォルト設定
# ========================================
IGNORE_PROTOCOL = True
SORT_QUERY_PARAMS = False
TARGET_URL_FILE = "urls.txt"
OUTPUT_FILE = "not_archived.txt"
CACHE_FILE = "archived_cdx.jsonl"
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
    
    # 3. スキームが無い場合は http:// を補完
    if '://' not in url:
        url = 'http://' + url
    
    # 4. URLをパース
    parsed = urlparse(url)
    
    # 5. スキームの正規化
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
        logger.info(f"キャッシュファイルを読み込み中: {cache_file}")
        try:
            lines = []
            with open(cache_file, 'r', encoding='utf-8') as f:
                logger.info("JSONL形式のキャッシュを読み込みます")
                for line in f:
                    if line.strip():
                        lines.append(json.loads(line))
            logger.info("JSONLキャッシュからデータを読み込みました")
            return lines
        except Exception as e:
            logger.warning(f"キャッシュ読み込みエラー: {e}")
            logger.info("CDX APIから再取得します...")
    
    logger.info(f"CDX APIからデータを取得中: {target_url}")
    api_url = "https://web.archive.org/cdx/search/cdx"
    
    params = {
        'url': target_url,
        'output': 'json',
        'filter': 'statuscode:200',
        'collapse': 'urlkey',
        'fl': 'original,timestamp'
    }
    
    headers = {
        'User-Agent': USER_AGENT
    }
    
    # SessionとRetry設定
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    try:
        response = session.get(api_url, params=params, headers=headers, timeout=600)
        response.raise_for_status()
        data = response.json()
        
        if not isinstance(data, list):
            raise CDXAPIError(
                f"CDX APIが予期しない型のレスポンスを返しました: {type(data).__name__}. "
                f"リストが期待されます。"
            )
        
        if len(data) == 0:
            logger.warning("CDX APIから空のレスポンスが返されました")
        
        Path(cache_file).parent.mkdir(parents=True, exist_ok=True)
        
        with open(cache_file, 'w', encoding='utf-8') as f:
            for row in data:
                f.write(json.dumps(row, ensure_ascii=False) + '\n')
        
        logger.info(f"CDX APIから {len(data)} 件のレコードを取得しました（JSONL形式で保存）")
        return data
        
    except requests.exceptions.RequestException as e:
        raise CDXAPIError(f"CDX API取得エラー: {e}") from e
    except json.JSONDecodeError as e:
        raise CDXAPIError(f"JSONパースエラー: {e}") from e


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
    # cdx_dataがlistでない場合は早期return（test_non_list_cdx_response対応）
    if not isinstance(cdx_data, list):
        logger.warning(f"cdx_dataがlist型ではありません: {type(cdx_data).__name__}")
        return set()
    
    archived_urls = set()
    original_idx = None
    is_first_row = True
    
    for row in cdx_data:
        # 行がlistでない場合はスキップして続行（異常行を無視）
        if not isinstance(row, list):
            logger.debug(f"非list行をスキップ: {type(row).__name__}")
            continue
        
        # 空行をスキップ
        if not row:
            continue
        
        # ヘッダー行の検出と処理
        if is_first_row or (row and row[0] in ('original', 'urlkey')):
            is_first_row = False
            
            # ヘッダー行から original カラムのインデックスを取得
            try:
                original_idx = row.index('original')
                logger.debug(f"originalカラムのインデックス: {original_idx}")
            except ValueError:
                # fl=original,timestamp の場合、通常は0番目
                logger.warning("'original'カラムが見つかりません。インデックス0を使用します")
                original_idx = 0
            continue  # ヘッダー行はスキップ
        
        # データ行の処理
        if original_idx is not None and len(row) > original_idx:
            original_url = row[original_idx]
            
            # URL が文字列であることを確認
            if isinstance(original_url, str):
                normalized = normalize_url(original_url, ignore_protocol, sort_query)
                archived_urls.add(normalized)
    
    return archived_urls


# ========================================
# メイン処理
# ========================================
def detect_not_archived(input_file: str, archived_urls: Set[str],
                        ignore_protocol: bool, sort_query: bool,
                        collect_archived: bool = False) -> tuple[List[str], Optional[List[str]], int]:
    """
    入力ファイルから未アーカイブURLを検出
    
    Args:
        input_file: 入力URLファイルのパス
        archived_urls: アーカイブ済みURLの集合（正規化済み）
        ignore_protocol: プロトコル無視フラグ
        sort_query: クエリソートフラグ
        collect_archived: アーカイブ済みURLも収集するか（デフォルト: False）
    
    Returns:
        tuple: (未アーカイブURLリスト, アーカイブ済みURLリストまたはNone, 入力URL総数)
    """
    if not os.path.exists(input_file):
        raise InputFileError(f"入力ファイルが見つかりません: {input_file}")
    
    not_archived = []
    archived = [] if collect_archived else None
    total_count = 0
    
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            original_url = line.strip()

            if not original_url:
                continue

            total_count += 1
            
            normalized = normalize_url(original_url, ignore_protocol, sort_query)
            
            if normalized not in archived_urls:
                not_archived.append(original_url)
            elif collect_archived:
                archived.append(original_url)
    
    return not_archived, archived, total_count


def main() -> int:
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
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='詳細なログ出力を有効化 (DEBUGレベル)'
    )
    
    args = parser.parse_args()
    
    # ロガー設定
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(levelname)s: %(message)s'
    )
    
    try:
        cdx_data = fetch_cdx_data(args.target_url, args.cache_file)
        
        archived_urls = extract_archived_urls(
            cdx_data,
            args.ignore_protocol,
            args.sort_query
        )
        cdx_raw_count = max(0, len(cdx_data) - 1)
        logger.info(f"CDX取得件数: {cdx_raw_count}")
        
        not_archived, archived, total_count = detect_not_archived(
            args.input_file,
            archived_urls,
            args.ignore_protocol,
            args.sort_query,
            collect_archived=bool(args.output_archived)
        )
        
        # 出力ファイルの親ディレクトリを自動作成
        Path(args.output_file).parent.mkdir(parents=True, exist_ok=True)
        with open(args.output_file, 'w', encoding='utf-8') as f:
            for url in not_archived:
                f.write(url + '\n')
        
        logger.info(f"調査対象URL数: {total_count}")
        logger.info(f"未アーカイブ件数: {len(not_archived)}")
        logger.info(f"結果を {args.output_file} に出力しました")

        if args.output_archived and archived is not None:
            Path(args.output_archived).parent.mkdir(parents=True, exist_ok=True)
            with open(args.output_archived, 'w', encoding='utf-8') as f:
                for url in archived:
                    f.write(url + '\n')
            logger.info(f"アーカイブ済みURLを {args.output_archived} に出力しました: {len(archived)} 件")
        
        return 0
    
    except CDXAPIError as e:
        logger.error(f"{e}")
        return 1
    except InputFileError as e:
        logger.error(f"{e}")
        return 1
    except Exception as e:
        logger.error(f"予期しないエラー: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
