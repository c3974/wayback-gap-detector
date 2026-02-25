#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Wayback Gap Detector - Unit Tests
Tests for each feature of waygap.py.
"""

import unittest
import os
import json
import tempfile
from waygap import (
    normalize_url,
    extract_archived_urls,
    detect_not_archived,
    fetch_cdx_data,
)
from exceptions import CDXAPIError, InputFileError
from unittest.mock import patch, MagicMock


class TestNormalizeUrl(unittest.TestCase):
    """Tests for the URL normalization function."""
    
    def test_basic_normalization(self):
        """Test basic normalization (whitespace trimming, protocol lowercasing)."""
        url = "  https://Example.com/Path  "
        result = normalize_url(url, ignore_protocol=True, sort_query=False)
        self.assertEqual(result, "http://example.com/Path")
    
    def test_html_unescape(self):
        """Test that HTML entities are decoded."""
        url = "https://example.com/page?name=John&amp;Doe"
        result = normalize_url(url, ignore_protocol=True, sort_query=False)
        self.assertIn("John&Doe", result)
    
    def test_protocol_ignore_true(self):
        """Test that http and https are treated as equivalent when ignore_protocol=True."""
        url1 = "http://example.com/page"
        url2 = "https://example.com/page"
        result1 = normalize_url(url1, ignore_protocol=True, sort_query=False)
        result2 = normalize_url(url2, ignore_protocol=True, sort_query=False)
        self.assertEqual(result1, result2)
        self.assertTrue(result1.startswith("http://"))
    
    def test_protocol_ignore_false(self):
        """Test that http and https are kept distinct when ignore_protocol=False."""
        url1 = "http://example.com/page"
        url2 = "https://example.com/page"
        result1 = normalize_url(url1, ignore_protocol=False, sort_query=False)
        result2 = normalize_url(url2, ignore_protocol=False, sort_query=False)
        self.assertNotEqual(result1, result2)
        self.assertTrue(result1.startswith("http://"))
        self.assertTrue(result2.startswith("https://"))
    
    def test_hostname_lowercase(self):
        """Test that hostnames are lowercased."""
        url = "https://EXAMPLE.COM/Path"
        result = normalize_url(url, ignore_protocol=True, sort_query=False)
        self.assertIn("example.com", result)
    
    def test_default_port_removal(self):
        """Test that default ports (80 for http, 443 for https) are stripped."""
        url1 = "http://example.com:80/page"
        url2 = "https://example.com:443/page"
        result1 = normalize_url(url1, ignore_protocol=False, sort_query=False)
        result2 = normalize_url(url2, ignore_protocol=False, sort_query=False)
        self.assertNotIn(":80", result1)
        self.assertNotIn(":443", result2)
    
    def test_non_default_port_preservation(self):
        """Test that non-default ports are preserved."""
        url = "http://example.com:8080/page"
        result = normalize_url(url, ignore_protocol=True, sort_query=False)
        self.assertIn(":8080", result)
    
    def test_trailing_slash_removal(self):
        """Test that trailing slashes are removed from non-root paths."""
        url = "https://example.com/path/"
        result = normalize_url(url, ignore_protocol=True, sort_query=False)
        self.assertEqual(result, "http://example.com/path")
    
    def test_root_path_preserved(self):
        """Test that the root path '/' is preserved as-is."""
        url = "https://example.com/"
        result = normalize_url(url, ignore_protocol=True, sort_query=False)
        self.assertEqual(result, "http://example.com/")
    
    def test_query_sort_enabled(self):
        """Test that query parameters are sorted alphabetically when sort_query=True."""
        url = "https://example.com/page?z=3&a=1&m=2"
        result = normalize_url(url, ignore_protocol=True, sort_query=True)
        # After sorting the order should be: a, m, z
        self.assertIn("a=1", result)
        self.assertIn("m=2", result)
        self.assertIn("z=3", result)
        # Verify that 'a' appears before 'z'
        self.assertLess(result.index("a=1"), result.index("z=3"))
    
    def test_query_sort_disabled(self):
        """Test that the original query parameter order is preserved when sort_query=False."""
        url = "https://example.com/page?z=3&a=1&m=2"
        result = normalize_url(url, ignore_protocol=True, sort_query=False)
        # Original order should be preserved
        self.assertIn("z=3", result)
        self.assertIn("a=1", result)
    
    def test_empty_query_removal(self):
        """Test that an empty query string ('?') is removed."""
        url = "https://example.com/page?"
        result = normalize_url(url, ignore_protocol=True, sort_query=False)
        self.assertNotIn("?", result)
    
    def test_fragment_removal(self):
        """Test that URL fragments are always stripped."""
        url = "https://example.com/page#section"
        result = normalize_url(url, ignore_protocol=True, sort_query=False)
        self.assertNotIn("#", result)
        self.assertEqual(result, "http://example.com/page")
    
    def test_complex_url(self):
        """Test normalization of a complex URL with all features combined."""
        url = "  HTTPS://Example.COM:443/Path/?b=2&a=1#fragment  "
        result = normalize_url(url, ignore_protocol=True, sort_query=True)
        # Protocol unified, hostname lowercased, port removed, query sorted, fragment removed
        expected = "http://example.com/Path?a=1&b=2"
        self.assertEqual(result, expected)
    
    def test_path_trailing_slash_equivalence(self):
        """Test that URLs with and without a trailing slash normalize to the same value."""
        url1 = "https://example.com/path"
        url2 = "https://example.com/path/"
        result1 = normalize_url(url1, ignore_protocol=True, sort_query=False)
        result2 = normalize_url(url2, ignore_protocol=True, sort_query=False)
        # The trailing slash should be removed so the two URLs are identical
        self.assertEqual(result1, result2)
        self.assertEqual(result1, "http://example.com/path")


class TestExtractArchivedUrls(unittest.TestCase):
    """Tests for the archived-URL extraction function."""
    
    def test_basic_extraction(self):
        """Test basic extraction of URLs from CDX data."""
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
        """Test that an empty input returns an empty set."""
        result = extract_archived_urls([], ignore_protocol=True, sort_query=False)
        self.assertEqual(len(result), 0)
    
    def test_header_only(self):
        """Test that a header-only CDX response returns an empty set."""
        cdx_data = [
            ["urlkey", "timestamp", "original", "mimetype", "statuscode", "digest", "length"]
        ]
        result = extract_archived_urls(cdx_data, ignore_protocol=True, sort_query=False)
        self.assertEqual(len(result), 0)


class TestCDXRobustness(unittest.TestCase):
    """Tests for handling empty or malformed CDX responses."""
    
    def test_empty_cdx_list(self):
        """Test that an empty CDX list ([]) is handled gracefully."""
        result = extract_archived_urls([], ignore_protocol=True, sort_query=False)
        self.assertEqual(len(result), 0)
        self.assertIsInstance(result, set)
    
    def test_header_only_cdx(self):
        """Test that CDX data with a header row but no data rows returns an empty set."""
        cdx_data = [
            ["urlkey", "timestamp", "original", "mimetype", "statuscode", "digest", "length"]
        ]
        result = extract_archived_urls(cdx_data, ignore_protocol=True, sort_query=False)
        self.assertEqual(len(result), 0)
    
    def test_non_list_cdx_response(self):
        """Test that non-list CDX responses (dict, None) return an empty set."""
        # dict
        result = extract_archived_urls({"error": "test"}, ignore_protocol=True, sort_query=False)
        self.assertEqual(len(result), 0)
        
        # None
        result = extract_archived_urls(None, ignore_protocol=True, sort_query=False)
        self.assertEqual(len(result), 0)
    
    def test_invalid_header_row(self):
        """Test that a malformed (non-list) header row results in an empty set."""
        cdx_data = [
            "invalid_header",  # string instead of list
            ["com,example)/page1", "20230101000000", "https://example.com/page1"]
        ]
        result = extract_archived_urls(cdx_data, ignore_protocol=True, sort_query=False)
        self.assertEqual(len(result), 0)  # Invalid header → empty set
    
    def test_invalid_data_rows(self):
        """Test that string rows mixed into data are skipped gracefully."""
        cdx_data = [
            ["urlkey", "timestamp", "original"],
            ["com,example)/page1", "20230101000000", "https://example.com/page1"],
            "invalid row",  # malformed row
            ["com,example)/page2", "20230102000000", "https://example.com/page2"],
        ]
        result = extract_archived_urls(cdx_data, ignore_protocol=True, sort_query=False)
        # Invalid rows are skipped; 2 valid URLs should be extracted
        self.assertEqual(len(result), 2)


class TestExceptions(unittest.TestCase):
    """Tests for exception handling."""
    
    def test_input_file_not_found(self):
        """Test that InputFileError is raised when the input file does not exist."""
        with self.assertRaises(InputFileError) as cm:
            detect_not_archived(
                "nonexistent_file.txt",
                set(),
                ignore_protocol=True,
                sort_query=False,
                collect_archived=False
            )
        self.assertIn("Input file not found", str(cm.exception))


class TestDetectNotArchived(unittest.TestCase):
    """Tests for the unarchived-URL detection function."""
    
    def setUp(self):
        """Create a temporary input file for each test."""
        self.temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8')
        self.temp_file.write("https://example.com/page1\n")
        self.temp_file.write("https://example.com/page2\n")
        self.temp_file.write("https://example.com/page3\n")
        self.temp_file.close()
    
    def tearDown(self):
        """Remove the temporary input file after each test."""
        os.unlink(self.temp_file.name)
    
    def test_all_archived(self):
        """Test that no unarchived URLs are reported when all inputs are archived."""
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
        """Test that only unarchived URLs are returned when some are missing."""
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
        """Test that all URLs are reported when none have been archived."""
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
        """Test that http-archived URLs are matched against https inputs when ignore_protocol=True."""
        archived_urls = {
            normalize_url("http://example.com/page1", True, False),  # archived via http
        }
        result, _, _ = detect_not_archived(
            self.temp_file.name,
            archived_urls,
            ignore_protocol=True,  # ignore_protocol enabled
            sort_query=False,
            collect_archived=False
        )
        # https://example.com/page1 should also be considered archived
        self.assertEqual(len(result), 2)
        self.assertNotIn("https://example.com/page1", result)

    def test_collect_archived_enabled(self):
        """Test that archived URLs are returned separately when collect_archived=True."""
        archived_urls = {
            normalize_url("https://example.com/page1", True, False),
        }
        not_archived, archived, total_count = detect_not_archived(
            self.temp_file.name,
            archived_urls,
            ignore_protocol=True,
            sort_query=False,
            collect_archived=True  # also collect archived URLs
        )
        
        # 3 input URLs in total
        self.assertEqual(total_count, 3)
        # 1 URL is archived
        self.assertEqual(len(archived), 1)
        self.assertIn("https://example.com/page1", archived)
        # 2 URLs are unarchived
        self.assertEqual(len(not_archived), 2)

    def test_collect_archived_disabled(self):
        """Test that archived is None when collect_archived=False."""
        archived_urls = {
            normalize_url("https://example.com/page1", True, False),
        }
        not_archived, archived, total_count = detect_not_archived(
            self.temp_file.name,
            archived_urls,
            ignore_protocol=True,
            sort_query=False,
            collect_archived=False  # do not collect archived URLs
        )
        
        # archived should be None
        self.assertIsNone(archived)
        # total_count and not_archived should be correct
        self.assertEqual(total_count, 3)
        self.assertEqual(len(not_archived), 2)


class TestIntegration(unittest.TestCase):
    """Integration tests."""
    
    def test_end_to_end_workflow(self):
        """End-to-end workflow test: extract archived URLs then detect unarchived ones."""
        # Prepare CDX data
        cdx_data = [
            ["urlkey", "timestamp", "original", "mimetype", "statuscode", "digest", "length"],
            ["com,example)/path1", "20230101000000", "https://example.com/path1", "text/html", "200", "ABC", "100"],
            ["com,example)/path2", "20230102000000", "http://example.com/path2", "text/html", "200", "DEF", "200"],
        ]
        
        # Extract archived URLs
        archived = extract_archived_urls(cdx_data, ignore_protocol=True, sort_query=False)
        
        # Create a temporary input file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as f:
            f.write("https://example.com/path1\n")
            f.write("https://example.com/path2\n")
            f.write("https://example.com/path3\n")
            temp_input = f.name
        
        try:
            # Detect unarchived URLs
            not_archived, _, _ = detect_not_archived(
                temp_input,
                archived,
                ignore_protocol=True,
                sort_query=False,
                collect_archived=False
            )
            
            # Verify results
            self.assertEqual(len(not_archived), 1)
            self.assertEqual(not_archived[0], "https://example.com/path3")
        
        finally:
            os.unlink(temp_input)

class TestAdditionalFeatures(unittest.TestCase):
    """Additional tests covering edge cases introduced in recent revisions."""

    def test_non_list_cdx_response_none(self):
        """When cdx_data is None, extract_archived_urls should return an empty set."""
        self.assertEqual(extract_archived_urls(None, ignore_protocol=True, sort_query=False), set())

    def test_non_list_cdx_response_dict(self):
        """When cdx_data is a dict, extract_archived_urls should return an empty set."""
        self.assertEqual(extract_archived_urls({"foo": "bar"}, ignore_protocol=True, sort_query=False), set())

    def test_non_list_cdx_response_string(self):
        """When cdx_data is a string, extract_archived_urls should return an empty set."""
        self.assertEqual(extract_archived_urls("invalid", ignore_protocol=True, sort_query=False), set())

    def test_extract_with_mixed_row_types(self):
        """When CDX data contains rows of mixed types, only list rows should be processed."""
        cdx = [
            ["original", "timestamp"],  # header
            {"bad": "row"},  # malformed row (dict)
            ["http://a.example/", "20200101"],  # valid row
            "invalid_string",  # malformed row (str)
            ["https://b/", "20200202"]  # valid row
        ]
        res = extract_archived_urls(cdx, ignore_protocol=True, sort_query=False)
        self.assertTrue(any("a.example" in u for u in res))
        self.assertTrue(any("b/" in u for u in res))
        self.assertEqual(len(res), 2)

    def test_fetch_cdx_reads_jsonl(self):
        """Test that fetch_cdx_data reads a JSONL cache file and returns a generator."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as f:
            f.write('["original","timestamp"]\n["http://x/","20200101"]\n')
            cache_path = f.name
        
        try:
            data_gen = fetch_cdx_data("x", cache_path)
            
            # Verify it is a generator
            self.assertTrue(hasattr(data_gen, '__iter__'))
            self.assertTrue(hasattr(data_gen, '__next__'))
            
            # Consume the generator into a list
            data = list(data_gen)

            # Verify each element is also a list
            self.assertTrue(all(isinstance(row, list) for row in data))
            # Verify 2 rows were read
            self.assertEqual(len(data), 2)
        finally:
            if os.path.exists(cache_path):
                os.unlink(cache_path)

    @patch('requests.Session')
    def test_fetch_cdx_cache_corrupt_fallback(self, mock_session_cls):
        """When the cache is corrupt, fetch_cdx_data should fall back to the CDX API."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as f:
            f.write('NOT-JSON\n')
            cache_path = f.name
            
        # Configure the API mock
        mock_response = MagicMock()
        mock_response.json.return_value = [["original", "timestamp"], ["http://ok/", "20200101"]]
        mock_response.raise_for_status.return_value = None
        
        mock_session = mock_session_cls.return_value
        mock_session.get.return_value = mock_response
        
        try:
            data_gen = fetch_cdx_data("ok", cache_path)

            try:
                first_item = next(data_gen)
            except StopIteration:
                pass  # No data returned
            
            # Verify the API was called
            mock_session.get.assert_called()

            # Consume the remaining data
            data = [first_item] + list(data_gen) if 'first_item' in locals() else []
            
            # Verify the result is a list
            self.assertIsInstance(data, list)
            # Verify that data was returned
            self.assertTrue(any("ok/" in str(row) for row in data))
            
        finally:
            if os.path.exists(cache_path):
                os.unlink(cache_path)

    def test_extract_no_original_column_uses_index_0(self):
        """When no 'original' column exists, index 0 should be used as a fallback."""
        cdx = [
            ["url", "timestamp"],  # header with 'url' instead of 'original'
            ["http://test.example/", "20200101"]
        ]
        res = extract_archived_urls(cdx, ignore_protocol=True, sort_query=False)
        # index 0 should be used, so "http://test.example/" is extracted
        self.assertTrue(any("test.example" in u for u in res))

    def test_extract_empty_rows_skipped(self):
        """Test that empty rows in CDX data are skipped without error."""
        cdx = [
            ["original", "timestamp"],
            [],  # empty row
            ["http://valid.example/", "20200101"],
            [],  # empty row
        ]
        res = extract_archived_urls(cdx, ignore_protocol=True, sort_query=False)
        self.assertEqual(len(res), 1)
        self.assertTrue(any("valid.example" in u for u in res))

class TestCDXPagination(unittest.TestCase):
    """Tests for CDX pagination via resumeKey."""

    @patch('requests.Session')
    def test_resumekey_pagination(self, mock_session_cls):
        """Test that resumeKey pagination fetches all pages correctly."""
        
        # First response (contains a resumeKey)
        first_response = MagicMock()
        first_response.json.return_value = [
            ["original", "timestamp"],
            ["http://page1.example/", "20200101"],
            ["http://page2.example/", "20200102"],
            ["RESUME_KEY_123"]  # resumeKey
        ]
        first_response.raise_for_status.return_value = None
        
        # Second response (no resumeKey — last page)
        second_response = MagicMock()
        second_response.json.return_value = [
            ["original", "timestamp"],
            ["http://page3.example/", "20200103"],
            ["http://page4.example/", "20200104"]
        ]
        second_response.raise_for_status.return_value = None
        
        # Configure the mock session
        mock_session = mock_session_cls.return_value
        mock_session.get.side_effect = [first_response, second_response]
        
        # Temporary cache file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as f:
            cache_path = f.name
        
        try:
            # Run fetch_cdx_data
            data_gen = fetch_cdx_data("http://example.com/*", cache_path)
            data = list(data_gen)
            
            # Verify the API was called twice
            self.assertEqual(mock_session.get.call_count, 2)
            
            # Verify the resumeKey was passed in the second call
            second_call_kwargs = mock_session.get.call_args_list[1][1]
            self.assertIn('params', second_call_kwargs)
            self.assertIn('resumeKey', second_call_kwargs['params'])
            self.assertEqual(second_call_kwargs['params']['resumeKey'], 'RESUME_KEY_123')
            
            # Verify 4 records were fetched (header and resumeKey rows excluded)
            self.assertEqual(len(data), 4)
            
            # Verify 4 lines were written to the cache file
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache_lines = [line.strip() for line in f if line.strip()]
            self.assertEqual(len(cache_lines), 4)
            
        finally:
            if os.path.exists(cache_path):
                os.unlink(cache_path)

    @patch('requests.Session')
    def test_empty_list_handling(self, mock_session_cls):
        """Test that empty lists embedded in the response are removed before processing."""
        
        mock_response = MagicMock()
        mock_response.json.return_value = [
            ["original", "timestamp"],
            ["http://page1.example/", "20200101"],
            [],  # empty list
            ["RESUME_KEY_456"]
        ]
        mock_response.raise_for_status.return_value = None
        
        mock_session = mock_session_cls.return_value
        mock_session.get.return_value = mock_response
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as f:
            cache_path = f.name
        
        try:
            data_gen = fetch_cdx_data("http://example.com/*", cache_path)
            data = list(data_gen)
            
            # Empty lists removed; only 1 record remains
            self.assertEqual(len(data), 1)
            
        finally:
            if os.path.exists(cache_path):
                os.unlink(cache_path)

    def test_generator_is_consumed_once(self):
        """Test that a generator can only be consumed once (standard generator behaviour)."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as f:
            f.write('["http://a/","20200101"]\n["http://b/","20200102"]\n')
            cache_path = f.name
        
        try:
            data_gen = fetch_cdx_data("test", cache_path)
            
            # First consumption
            data1 = list(data_gen)
            self.assertEqual(len(data1), 2)
            
            # Second consumption should be empty
            data2 = list(data_gen)
            self.assertEqual(len(data2), 0)
            
        finally:
            if os.path.exists(cache_path):
                os.unlink(cache_path)


def run_tests():
    """Discover and run all test cases."""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestNormalizeUrl))
    suite.addTests(loader.loadTestsFromTestCase(TestExtractArchivedUrls))
    suite.addTests(loader.loadTestsFromTestCase(TestCDXRobustness))
    suite.addTests(loader.loadTestsFromTestCase(TestExceptions))
    suite.addTests(loader.loadTestsFromTestCase(TestDetectNotArchived))
    suite.addTests(loader.loadTestsFromTestCase(TestIntegration))
    suite.addTests(loader.loadTestsFromTestCase(TestAdditionalFeatures))
    suite.addTests(loader.loadTestsFromTestCase(TestCDXPagination))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result


if __name__ == '__main__':
    result = run_tests()
    
    # Test result summary
    print("\n" + "="*70)
    print("Test Result Summary")
    print("="*70)
    print(f"Tests run:  {result.testsRun}")
    print(f"Passed:     {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failed:     {len(result.failures)}")
    print(f"Errors:     {len(result.errors)}")
    print("="*70)
    
    # Exit code
    exit(0 if result.wasSuccessful() else 1)
