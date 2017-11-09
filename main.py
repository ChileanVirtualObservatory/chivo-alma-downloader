#!/usr/bin/env python
# -*- coding: utf-8 -*-

from downloader import Downloader

download = Downloader();

#download.clear_db()
download.saveMetadata(search='', date=None)

# get last release date, useful for new searches:
#print(download.get_project_release_date())