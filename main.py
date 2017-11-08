#!/usr/bin/env python
# -*- coding: utf-8 -*-

from downloader import Downloader

download = Downloader();

#download.clear_db()
download.saveMetadata(q='', date=None)
#print(download.get_project_release_date())