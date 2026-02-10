#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Wayback Gap Detector - 単体テスト
wbgap.pyの各機能をテストする
"""

import unittest
import os
import json
import tempfile
from wbgap import (
    normalize_url,
    extract_archived_urls,
    detect_not_archived,
)
from exceptions import CDXAPIError, InputFileError


class TestNormalizeUrl(unittest.TestCase):
    """URL正規化関数のテスト"""
    
    def test_basic_normalization(self):
        """基本的な正規化のテスト"""
        url = "  https://Example.com/Path  "
        result = normalize_url(url, ignore_protocol=True, sort_query=False)
        self.assertEqual(result, "http://example.com/Path")
    
    def test_html_unescape(self):
        """HTML実体参照のデコードテスト"""
        url = "https://example.com/page?name=John&amp;Doe"
        result = normalize_url(url, ignore_protocol=True, sort_query=False)
        self.assertIn("John&Doe", result)
    
    def test_protocol_ignore_true(self):
        """プロトコル無視機能のテスト（有効）"""
        url1 = "http://example.com/page"
        url2 = "https://example.com/page"
        result1 = normalize_url(url1, ignore_protocol=True, sort_query=False)
        result2 = normalize_url(url2, ignore_protocol=True, sort_query=False)
        self.assertEqual(result1, result2)
        self.assertTrue(result1.startswith("http://"))
    
    def test_protocol_ignore_false(self):
        """プロトコル無視機能のテスト（無効）"""
        url1 = "http://example.com/page"
        url2 = "https://example.com/page"
        result1 = normalize_url(url1, ignore_protocol=False, sort_query=False)
        result2 = normalize_url(url2, ignore_protocol=False, sort_query=False)
        self.assertNotEqual(result1, result2)
        self.assertTrue(result1.startswith("http://"))
        self.assertTrue(result2.startswith("https://"))
    
    def test_hostname_lowercase(self):
        """ホスト名の小文字化テスト"""
        url = "https://EXAMPLE.COM/Path"
        result = normalize_url(url, ignore_protocol=True, sort_query=False)
        self.assertIn("example.com", result)
    
    def test_default_port_removal(self):
        """デフォルトポート削除のテスト"""
        url1 = "http://example.com:80/page"
        url2 = "https://example.com:443/page"
        result1 = normalize_url(url1, ignore_protocol=False, sort_query=False)
        result2 = normalize_url(url2, ignore_protocol=False, sort_query=False)
        self.assertNotIn(":80", result1)
        self.assertNotIn(":443", result2)
    
    def test_non_default_port_preservation(self):
        """非デフォルトポート保持のテスト"""
        url = "http://example.com:8080/page"
        result = normalize_url(url, ignore_protocol=True, sort_query=False)
        self.assertIn(":8080", result)
    
    def test_trailing_slash_removal(self):
        """末尾スラッシュ削除のテスト"""
        url = "https://example.com/path/"
        result = normalize_url(url, ignore_protocol=True, sort_query=False)
        self.assertEqual(result, "http://example.com/path")
    
    def test_root_path_preserved(self):
        """ルートパスの保持テスト"""
        url = "https://example.com/"
        result = normalize_url(url, ignore_protocol=True, sort_query=False)
        self.assertEqual(result, "http://example.com/")
    
    def test_query_sort_enabled(self):
        """クエリパラメータソート有効のテスト"""
        url = "https://example.com/page?z=3&a=1&m=2"
        result = normalize_url(url, ignore_protocol=True, sort_query=True)
        # ソート後はa, m, zの順になる
        self.assertIn("a=1", result)
        self.assertIn("m=2", result)
        self.assertIn("z=3", result)
        # a が z より前に来ることを確認
        self.assertLess(result.index("a=1"), result.index("z=3"))
    
    def test_query_sort_disabled(self):
        """クエリパラメータソート無効のテスト"""
        url = "https://example.com/page?z=3&a=1&m=2"
        result = normalize_url(url, ignore_protocol=True, sort_query=False)
        # 元の順序を維持
        self.assertIn("z=3", result)
        self.assertIn("a=1", result)
    
    def test_empty_query_removal(self):
        """空クエリの削除テスト"""
        url = "https://example.com/page?"
        result = normalize_url(url, ignore_protocol=True, sort_query=False)
        self.assertNotIn("?", result)
    
    def test_fragment_removal(self):
        """フラグメント削除のテスト"""
        url = "https://example.com/page#section"
        result = normalize_url(url, ignore_protocol=True, sort_query=False)
        self.assertNotIn("#", result)
        self.assertEqual(result, "http://example.com/page")
    
    def test_complex_url(self):
        """複雑なURLの正規化テスト"""
        url = "  HTTPS://Example.COM:443/Path/?b=2&a=1#fragment  "
        result = normalize_url(url, ignore_protocol=True, sort_query=True)
        # プロトコル統一、小文字化、ポート削除、クエリソート、フラグメント削除
        expected = "http://example.com/Path?a=1&b=2"
        self.assertEqual(result, expected)
    
    def test_path_trailing_slash_equivalence(self):
        """パス末尾スラッシュの同一性テスト"""
        url1 = "https://example.com/path"
        url2 = "https://example.com/path/"
        result1 = normalize_url(url1, ignore_protocol=True, sort_query=False)
        result2 = normalize_url(url2, ignore_protocol=True, sort_query=False)
        # 末尾スラッシュが削除されて同一にURLになる
        self.assertEqual(result1, result2)
        self.assertEqual(result1, "http://example.com/path")


class TestExtractArchivedUrls(unittest.TestCase):
    """アーカイブ済みURL抽出機能のテスト"""
    
    def test_basic_extraction(self):
        """基本的な抽出テスト"""
        cdx_data = [
            ["urlkey", "timestamp", "original", "mimetype", "statuscode", "digest", "length"],
            ["com,example)/page1", "20230101000000", "https://example.com/page1", "text/html", "200", "ABC123", "1234"],
            ["com,example)/page2", "20230102000000", "https://example.com/page2", "text/html", "200", "DEF456", "5678"],
        ]
        result = extract_archived_urls(cdx_data, ignore_protocol=True, sort_query=False)
        self.assertEqual(len(result), 2)
        self.assertIn(normalize_url("https://example.com/page1", True, False), result)
        self.assertIn(normalize_url("https://example.com/page2", True, False), result)
    
    def test_empty_data(self):
        """空データのテスト"""
        result = extract_archived_urls([], ignore_protocol=True, sort_query=False)
        self.assertEqual(len(result), 0)
    
    def test_header_only(self):
        """ヘッダーのみのテスト"""
        cdx_data = [
            ["urlkey", "timestamp", "original", "mimetype", "statuscode", "digest", "length"]
        ]
        result = extract_archived_urls(cdx_data, ignore_protocol=True, sort_query=False)
        self.assertEqual(len(result), 0)


class TestCDXRobustness(unittest.TestCase):
    """空や異常なCDXレスポンスのテスト"""
    
    def test_empty_cdx_list(self):
        """空CDXリスト（[]）の処理"""
        result = extract_archived_urls([], ignore_protocol=True, sort_query=False)
        self.assertEqual(len(result), 0)
        self.assertIsInstance(result, set)
    
    def test_header_only_cdx(self):
        """ヘッダーのみのCDXデータ"""
        cdx_data = [
            ["urlkey", "timestamp", "original", "mimetype", "statuscode", "digest", "length"]
        ]
        result = extract_archived_urls(cdx_data, ignore_protocol=True, sort_query=False)
        self.assertEqual(len(result), 0)
    
    def test_non_list_cdx_response(self):
        """非リスト型のCDXレスポンス（dict, str, None）"""
        # dict
        result = extract_archived_urls({"error": "test"}, ignore_protocol=True, sort_query=False)
        self.assertEqual(len(result), 0)
        
        # None
        result = extract_archived_urls(None, ignore_protocol=True, sort_query=False)
        self.assertEqual(len(result), 0)
    
    def test_invalid_header_row(self):
        """ヘッダー行が文字列などの異常ケース"""
        cdx_data = [
            "invalid_header",  # 文字列
            ["com,example)/page1", "20230101000000", "https://example.com/page1"]
        ]
        result = extract_archived_urls(cdx_data, ignore_protocol=True, sort_query=False)
        self.assertEqual(len(result), 0)  # ヘッダーが無効なので空セット
    
    def test_invalid_data_rows(self):
        """データ行に文字列が混入している場合"""
        cdx_data = [
            ["urlkey", "timestamp", "original"],
            ["com,example)/page1", "20230101000000", "https://example.com/page1"],
            "invalid row",  # 異常行
            ["com,example)/page2", "20230102000000", "https://example.com/page2"],
        ]
        result = extract_archived_urls(cdx_data, ignore_protocol=True, sort_query=False)
        # 無効な行はスキップされ、2件の有効なURLが抽出される
        self.assertEqual(len(result), 2)


class TestExceptions(unittest.TestCase):
    """例外処理のテスト"""
    
    def test_input_file_not_found(self):
        """入力ファイルが存在しない場合にInputFileErrorが発生"""
        with self.assertRaises(InputFileError) as cm:
            detect_not_archived(
                "nonexistent_file.txt",
                set(),
                ignore_protocol=True,
                sort_query=False,
                collect_archived=False
            )
        self.assertIn("入力ファイルが見つかりません", str(cm.exception))


class TestDetectNotArchived(unittest.TestCase):
    """未アーカイブURL検出機能のテスト"""
    
    def setUp(self):
        """テスト用の一時ファイルを作成"""
        self.temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8')
        self.temp_file.write("https://example.com/page1\n")
        self.temp_file.write("https://example.com/page2\n")
        self.temp_file.write("https://example.com/page3\n")
        self.temp_file.close()
    
    def tearDown(self):
        """一時ファイルを削除"""
        os.unlink(self.temp_file.name)
    
    def test_all_archived(self):
        """すべてアーカイブ済みの場合"""
        archived_urls = {
            normalize_url("https://example.com/page1", True, False),
            normalize_url("https://example.com/page2", True, False),
            normalize_url("https://example.com/page3", True, False),
        }
        result, _, _ = detect_not_archived(
            self.temp_file.name,
            archived_urls,
            ignore_protocol=True,
            sort_query=False,
            collect_archived=False
        )
        self.assertEqual(len(result), 0)
    
    def test_some_not_archived(self):
        """一部が未アーカイブの場合"""
        archived_urls = {
            normalize_url("https://example.com/page1", True, False),
        }
        result, _, _ = detect_not_archived(
            self.temp_file.name,
            archived_urls,
            ignore_protocol=True,
            sort_query=False,
            collect_archived=False
        )
        self.assertEqual(len(result), 2)
        self.assertIn("https://example.com/page2", result)
        self.assertIn("https://example.com/page3", result)
    
    def test_all_not_archived(self):
        """すべて未アーカイブの場合"""
        archived_urls = set()
        result, _, _ = detect_not_archived(
            self.temp_file.name,
            archived_urls,
            ignore_protocol=True,
            sort_query=False,
            collect_archived=False
        )
        self.assertEqual(len(result), 3)
    
    def test_protocol_independence(self):
        """プロトコル無視での検出テスト"""
        archived_urls = {
            normalize_url("http://example.com/page1", True, False),  # httpでアーカイブ
        }
        result, _, _ = detect_not_archived(
            self.temp_file.name,
            archived_urls,
            ignore_protocol=True,  # プロトコル無視有効
            sort_query=False,
            collect_archived=False
        )
        # https://example.com/page1 もアーカイブ済みとみなされる
        self.assertEqual(len(result), 2)
        self.assertNotIn("https://example.com/page1", result)

    def test_collect_archived_enabled(self):
        """アーカイブ済みURL収集機能のテスト"""
        archived_urls = {
            normalize_url("https://example.com/page1", True, False),
        }
        not_archived, archived, total_count = detect_not_archived(
            self.temp_file.name,
            archived_urls,
            ignore_protocol=True,
            sort_query=False,
            collect_archived=True  # アーカイブ済みも収集
        )
        
        # 合計3件の入力URL
        self.assertEqual(total_count, 3)
        # 1件がアーカイブ済み
        self.assertEqual(len(archived), 1)
        self.assertIn("https://example.com/page1", archived)
        # 2件が未アーカイブ
        self.assertEqual(len(not_archived), 2)

    def test_collect_archived_disabled(self):
        """アーカイブ済みURL収集なしのテスト"""
        archived_urls = {
            normalize_url("https://example.com/page1", True, False),
        }
        not_archived, archived, total_count = detect_not_archived(
            self.temp_file.name,
            archived_urls,
            ignore_protocol=True,
            sort_query=False,
            collect_archived=False  # 収集しない
        )
        
        # archivedはNone
        self.assertIsNone(archived)
        # total_countとnot_archivedは正しい
        self.assertEqual(total_count, 3)
        self.assertEqual(len(not_archived), 2)


class TestIntegration(unittest.TestCase):
    """統合テスト"""
    
    def test_end_to_end_workflow(self):
        """エンドツーエンドのワークフローテスト"""
        # CDXデータの準備
        cdx_data = [
            ["urlkey", "timestamp", "original", "mimetype", "statuscode", "digest", "length"],
            ["com,example)/path1", "20230101000000", "https://example.com/path1", "text/html", "200", "ABC", "100"],
            ["com,example)/path2", "20230102000000", "http://example.com/path2", "text/html", "200", "DEF", "200"],
        ]
        
        # アーカイブ済みURL抽出
        archived = extract_archived_urls(cdx_data, ignore_protocol=True, sort_query=False)
        
        # 入力ファイル作成
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as f:
            f.write("https://example.com/path1\n")
            f.write("https://example.com/path2\n")
            f.write("https://example.com/path3\n")
            temp_input = f.name
        
        try:
            # 未アーカイブ検出
            not_archived, _, _ = detect_not_archived(
                temp_input,
                archived,
                ignore_protocol=True,
                sort_query=False,
                collect_archived=False
            )
            
            # 検証
            self.assertEqual(len(not_archived), 1)
            self.assertEqual(not_archived[0], "https://example.com/path3")
        
        finally:
            os.unlink(temp_input)


def run_tests():
    """テストを実行"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # すべてのテストクラスを追加
    suite.addTests(loader.loadTestsFromTestCase(TestNormalizeUrl))
    suite.addTests(loader.loadTestsFromTestCase(TestExtractArchivedUrls))
    suite.addTests(loader.loadTestsFromTestCase(TestCDXRobustness))
    suite.addTests(loader.loadTestsFromTestCase(TestExceptions))
    suite.addTests(loader.loadTestsFromTestCase(TestDetectNotArchived))
    suite.addTests(loader.loadTestsFromTestCase(TestIntegration))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result


if __name__ == '__main__':
    result = run_tests()
    
    # テスト結果のサマリー
    print("\n" + "="*70)
    print("テスト結果サマリー")
    print("="*70)
    print(f"実行テスト数: {result.testsRun}")
    print(f"成功: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"失敗: {len(result.failures)}")
    print(f"エラー: {len(result.errors)}")
    print("="*70)
    
    # 終了コード
    exit(0 if result.wasSuccessful() else 1)
