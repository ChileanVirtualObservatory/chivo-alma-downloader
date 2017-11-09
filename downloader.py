#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, errno, re, sys
from astroquery.alma import Alma
import numpy as np
from pprint import pprint
import sqlite3 as lite

def make_sure_path_exists(path):
	try:
		os.makedirs(path)
	except OSError as exception:
		if exception.errno != errno.EEXIST:
			raise

class Downloader:
	def __init__(self):
		self.db_directory = '.db/alma.db'
		self.download_directory = '/tmp/chiv-alma-downloader/'
		self._connection = None

	# CREATE SQLITE CONNECTION
	def __create_sqlite_connection(self):
		if self._connection is None:
			self._connection = lite.connect(self.db_directory)

	# DESTROY SQLITE CONNECTION
	def __destroy_sqlite_connection(self):
		if self._connection is not None:
			self._connection.close()
			self._connection = None

	# TRUNCATE ALL OR SPECIFIC TABLE
	def clear_db(self, table=None):
		self.__create_sqlite_connection()
		with self._connection:
			cur = self._connection.cursor()
			if table:
				cur.execute("DELETE FROM ?;", [table])
			else:
				cur.execute("DELETE FROM projects_data;")
				cur.execute("DELETE FROM links_list;")
		self.__destroy_sqlite_connection()

	def get_projects(self, table=None):
		self.__create_sqlite_connection()
		result = None
		with self._connection:
			cur = self._connection.cursor()
			cur.execute("SELECT * FROM projects_data;")
			result = cur.fetchone()
		self.__destroy_sqlite_connection()
		return result

	def get_links(self, table=None):
		self.__create_sqlite_connection()
		result = None
		with self._connection:
			cur = self._connection.cursor()
			cur.execute("SELECT * FROM links_list;")
			result = cur.fetchone()
		self.__destroy_sqlite_connection()
		return result

	def get_project_release_date(self):
		self.__create_sqlite_connection()
		r = None
		with self._connection:
			cur = self._connection.cursor()
			cur.execute("SELECT release_date FROM projects_data ORDER BY release_date DESC LIMIT 1;")
			result = cur.fetchone()
			if result:
				r = result[0]
		self.__destroy_sqlite_connection()
		return r

	def saveMetadata(self, search='', save_link_list=True, download=False, date=None):
		# date is the Release Date from the project

		self.__create_sqlite_connection()
		with self._connection:
			cur = self._connection.cursor()

			# ----------------------------------
			# | GETTING DATA FROM ALMA ARCHIVE |
			# ----------------------------------
			if date:
				data = Alma.query_object(search)
			else:
				data = Alma.query_object(search)
			print('[Downloader] Number of results:', len(data))

			i = 0
			cols_translations = dict()

			# -----------------------------
			# | GETTING ID FOR INSERTIONS |
			# -----------------------------
			cur.execute("SELECT id_projects_data AS id FROM projects_data ORDER BY id_projects_data DESC LIMIT 1;")
			result = cur.fetchone()
			if result:
				next_project_id = result[0] + 1
			else:
				next_project_id = 1
			cur.execute("SELECT id_link FROM links_list ORDER BY id_link DESC LIMIT 1;")
			result = cur.fetchone()
			if result:
				next_link_id = result[0] + 1
			else:
				next_link_id = 1

			# ---------------------------
			# | GETTING RESULT COLNAMES |
			# ---------------------------
			#print('Number of colums:', len(data.columns))
			for col in data.columns:
				new_colname = re.sub('\(.*\)', '', data[col].name).strip()
				new_colname = new_colname.replace(" ", "_").lower();
				cols_translations[data[col].name] = new_colname


			# ---------------------------------
			# | GETTING ALL PROJECTS METADATA |
			# ---------------------------------
			tmp_col_data = []
			tmp_mousid = set()
			print("[Downloader] Saving all metadata of projects ...")
			for row in data:
				i+=1

				tmp_col_data.append(next_project_id)
				for col in row.columns:
					if isinstance(row[col], np.ma.core.MaskedArray):
						tmp_col_data.append(np.ma.dumps(row[col]))
					else:
						tmp_col_data.append(str(row[col]))

				try:
					cur.execute("INSERT INTO projects_data VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", tmp_col_data)
					print('(ProjectData) Insertion number ['+str(i)+']; ID='+str(next_project_id))
					next_project_id+=1
				except lite.Error, e:
					print "Error: %s" % e.args[0]
					raise
				del tmp_col_data[:]
				
				tmp_mousid.add(row['Member ous id'])

				# Test with only one project-row:
				#if i == 1:
				#	break

			# -------------------------------
			# | GETTING ALL LINKS FROM UIDS |
			# -------------------------------
			print("[Downloader] Saving all links from projects-uids ...")
			if save_link_list:
				for mous in tmp_mousid:
					uids = np.unique(mous)

					link_list = Alma.stage_data(uids)
					for link in link_list:
						try:
							cur.execute("INSERT INTO links_list VALUES(?, ?, ?, ?)", [next_link_id, link['URL'], link['uid'], link['size']])
							print('       (Link) RowDB_ID='+str(next_link_id))
							next_link_id+=1
						except lite.Error, e:
							print "Error: %s" % e.args[0]
							raise
					#print(link_list['size'].sum())

					if download:
						print('[Downloader] Downloading tars ...')
						myAlma = Alma()
						make_sure_path_exists(self.download_directory)
						myAlma.cache_location = self.download_directory
						myAlma.download_files(link_list, cache=True)
		self.__destroy_sqlite_connection()