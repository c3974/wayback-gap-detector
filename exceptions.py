#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Wayback Gap Detector - Custom Exceptions
ライブラリ層で使用するカスタム例外クラス
"""


class WaybackGapDetectorError(Exception):
    """Base exception for Wayback Gap Detector"""
    pass


class CDXAPIError(WaybackGapDetectorError):
    """CDX API related errors (network, response parsing, etc.)"""
    pass


class InputFileError(WaybackGapDetectorError):
    """Input file related errors (not found, encoding issues, etc.)"""
    pass
