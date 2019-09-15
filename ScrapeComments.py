# -*- coding: utf-8 -*-
# Author: Aarash Heydari/aarashy@gmail.com with help from https://developers.google.com/youtube/v3/docs/

'''
	This is a script which scrapes YouTube comments
	from a video, playlist or channel and saves them into a pickle file.

	Because this script calls the YouTube API, you need to authenticate your requests from your own Google account.
	See the instructions here to acquire a necessary client_secrets.json file:
	https://developers.google.com/analytics/devguides/config/mgmt/v3/quickstart/installed-py

	When the script runs, every chosen video is added to a nested dictionary and list object.
	It was meant to be similar to a JSON format but I acknowledge it is not the greatest format.
	We transformed the data into CSV format before doing analytics. 

	The generated data is saved in .pkl as a dictionary of the following format: 
	{Video Title : (videoID, comments,
	 					(publish_date, channel_title, duration, view_count, 
	 					like_count, dislike_count, favorite_count, date_scraped))}

	The Video Titles are the keys, which each point to a tuple of three elements: Video ID,
	a list of comments, and some metadata statistics.

	Each item in the list of comments is a dictionary providing metadata about the comment,
	the text of the comment, and a list of replies to that comment. The format looks like this:

	{"original comment": [(author, timestamp, like_count), text],
	     "replies": [[(author, timestamp, like_count), text for each reply]]}

	#################
	# Usage Options #
	#################

	1. Use a vid. Manually find and feed Video IDs with the (add_video_response_to_dictionary function)

	2. Use a pid. Manually find a playlist ID and feed with the (get_video_ids_from_playlist_id function)

	3. Use a cid. Manually find a channel ID and either 
		3a. Scrape all playlists of the channel with the (playlists_list_by_channel_id function)

		3b. Scrape all videos in order of recency with the two command line arguments -videosbychannel cid
		Uses (get_all_uploads_from_channel_id function)

	4. Use a vid. THIS IS MY PREFERRED WAY OF USING THE SCRIPT. 
		Retrieve a channel ID using the Video ID of a video from that channel,
			then scrape all the videos of the channel using the (get_channel_id_from_video_id function)
	################
'''


import os
import pandas
import google.oauth2.credentials

import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow

from termcolor import colored
import datetime
import pickle

'''
Make edits to the run function to specify your usage mode
	(i.e. whether you are scraping by channel, playlist, or individual Video ID)

You can find Video, Playlist, and Channel IDs embedded in YouTube URLs,
	though Channel IDs may be more easily found using the (get_channel_id_from_video_id function)
'''
def run():
	# When running locally, disable OAuthlib's HTTPs verification. When
	# running in production *do not* leave this option enabled.
	os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

	# We want to ignore videos released in the last two weeks.
	current_date = datetime.datetime.now()
	older_than = str(current_date - datetime.timedelta(days=14))
	current_date = str(current_date)
	current_date = current_date[:10] + 'T' + current_date[12:current_date.index('.') + 4] + 'Z'
	older_than = older_than[:10] + 'T' + older_than[12:older_than.index('.') + 4] + 'Z'

	#################################################
	# Multi Video ID Starter Code: These are 100 Russia Today videos from around April 2018.
	# dct = {}
	# v_IDs =  ["U5z_aCNBNsg", "WMXi5nFgo2k", "QsB-FGQoTfQ", "1Afz-hBH_a4", "nr_bwP9y580"]
	# v_IDs += ["rIussGPcPMQ", "lW8Hy5K8FPU", "hMr_YNxtSOY", "T4cJh7OAmks", "52yOKCARr54"]
	# v_IDs += ["b13FS0rQVQw", "yRFLsRCyKeA", "VO3His4mhEw", "qUJNg8PVRQU", "-TqgQd1cLl8"]
	# v_IDs += ["bHAd_VxPDnw", "5vwnAsvGOKc", "0ZfnEO2gWo0", "R3lbak_vVh4", "SgjMeD06pfQ"]
	# v_IDs += ["5ib3N8hTa0Y", "RyA0QZHqzS8", "-hWxY6kjdkQ", "dxHG5OCXohU", "PniMT2NTnvY"]
	# v_IDs += ["n6ox0ZbvlN0", "92uG4DTOUk4", "RYyR1xz-6fo", "m8PvnsrfdZA", "polKuVNAKEA"]
	# v_IDs += ["xDGYyHZGSxQ", "iPDJKZfqaX8", "k20zAeRhCgw", "uYWNXhOyRrg", "BG3k4UBdV0Y"]
	# v_IDs += ["4d90d9rg1XI", "RQeizweazMk", "qS0JhecyZGI", "0LzDGDUahO4", "jIrH6OL-ySc"]
	# v_IDs += ["0LhBaFpASvk", "MCGz5ml_m7Y", "57vqbFqHsQE", "ikX4BKQ5Sqg", "wX0AtCZFXCA"]
	# v_IDs += ["4S-1srEtdWo", "22j1EsV-Wmo", "AyR5kVB9PXo", "RGqS1AgMTX0", "LpXW1dgwMx0"]
	# #
	# v_IDs += ["IRqex2IIc_s", "DDmq3EUMjDA", "2MqG0XDRpwQ", "-2wf_qvXVKc", "S9LUkmE1ayE"]
	# v_IDs += ["lYSGf-Ia1-8", "cFG8r4kyN4Q", "t9n5irwWHfI", "wtdHXONNj8g", "9U1hi77OHyM"]
	# v_IDs += ["fnDbEM-FrpE", "yClMsr6ZfvM", "dCdiVIIDm4I", "jsE-5TnCSog", "lbmguY_1T4I"]
	# v_IDs += ["F1nZYCZ0JcU", "exxPIIKZ6IE", "52deluDn5vQ", "7ATdRpvQKvI", "39cNABKP5Q0"]
	# v_IDs += ["OAVjDiYyz1c", "4_ujTVH2IwM", "xEhUWxHjzIQ", "7-jQk0-grNs", "hykDH37Suj0"]
	# v_IDs += ["GF1_EnTmcc0", "ApyrBOBG84s", "Q8Iw8bIA6yk", "-yxZJseb5Mg", "dxRiG8vRRBk"]
	# v_IDs += ["iz2pJRAaaB8", "LuQ3mplRNLc", "C-Y2pIYeomg", "G1_4D4xXkAc", "kWavw9xyK3g"]
	# v_IDs += ["dLzq_ioJGd8", "E4LCvFa4gQo", "4baXE_H0KA4", "do-tvrhF6Rg", "CptbnSSz6Ls"]
	# v_IDs += ["v1mJPg4op0s", "vWYcIzl9IPE", "yOIqSn-dFfQ", "zFNnhRMvlQA", "59QFVE37UGE"]
	# v_IDs += ["TzsjH5nn4Cc", "ga2x38jZoqc", "KGps81N6pL0", "Gvi7owj3dvU", "mo0l8A66ILc"]
	# for v_id in v_IDs:
	#	try:
	# 		add_response_to_dictionary(dct, v_id, current_date, older_than)
	#	except:
	#		print("error encountered for video: ", v_id)
	#		continue
	# save_data(dct, "russia_today_comments")

	########################################################
	# Single Playlist Starter Code: Hard code a p_id and scrape the playlist.
	# 
	# dct = load_data("prager_u_playlist")
	# p_id = 'PLIBtb_NuIJ1w_5qAEs5cSUJ5Bk0R8QLaY'
	# print("old len playlist: ", len(dct))
	# v_IDs = get_video_ids_from_playlist_id(p_id)
	# print("num v_ids:", len(v_IDs))
	# for i in range(len(v_IDs)):
	# 	print("adding response for ", v_IDs[i], " (%d out of %d)" % (i, len(v_IDs)))
	# 	try:
	# 		add_response_to_dictionary(dct, v_IDs[i], current_date, older_than)
	# 	except Exception as e:
	# 		print("Error with Video ID = %s. Error message:" % v_IDs[i], e)
	# save_data(dct, "prager_u_playlist")
	# print("done! len of playlist is now", len(dct))

	#########################################################
	# Single Channel Starter Code: Hard code a Channel ID
	#
	# c_id = "UCZWlSUNDvCCS1hBiXV0zKcA" # PragerU's Channel ID
	# dct = {}
	# v_ids = get_videos_from_playlists_from_channel_id(dct, c_id)
	# save_data(dct, "prager_u_playlist")
	# n = len(v_ids)
	# for i in range(n):
	# 	if len(dct) > 200:
	# 		break
	# 	print("video %d out of %d: %s" % (i, n, v_ids[i]))
	# 	add_response_to_dictionary(dct, v_ids[i], current_date, older_than)
	# save_data(dct, "fox_news_comments")
	#########################################################

	# dct = {}
	# v_id = 'exvttEBIQfY' # Fox
	# c_id = get_channel_id_from_video_id(v_id)
	# p_id = get_all_uploads_from_channel_id(c_id)
	# v_ids = get_video_ids_from_playlist_id(p_id, 750)
	# n = len(v_ids)
	# try:
	# 	for i in range(290, n):
	# 		if len(dct) > 199:
	# 			break
	# 		print("video %d out of %d: %s" % (i, n, v_ids[i]))
	# 		add_response_to_dictionary(dct, v_ids[i], current_date, older_than)
	# except KeyboardInterrupt:
	# 	print("Stopped early with %d videos" % len(dct))
	# except Exception as e:
	# 	print("Unexpected Error", e)
	# 	print("Stopped early with %d videos" % len(dct))
	# save_data(dct, "fox_news_comments")

	# dct = {}
	# v_id = '_wR_7XyB6eM' # Breitbart
	# c_id = get_channel_id_from_video_id(v_id)
	# p_id = get_all_uploads_from_channel_id(c_id)
	# v_ids = get_video_ids_from_playlist_id(p_id, 400)
	# n = len(v_ids)
	# for i in range(n):
	# 	if len(dct) > 200:
	# 		break
	# 	print("video %d out of %d: %s" % (i, n, v_ids[i]))
	# 	add_response_to_dictionary(dct, v_ids[i], current_date, older_than)
	# save_data(dct, "breitbart_comments")

	
	# dct = load_data("cnn_comments") # CNN
	# v_ids = ["htl2v3YkjOQ", "FzZ0ltuMgzc", "xkmIKnQFnlI", "JmulymNUBEk", "guk_p172c58", "zdFe-LmFRV8"] # CNN
	# n = len(v_ids)
	# try:
	# 	for i in range(300, n):
	# 		v = add_response_to_dictionary(dct, v_id[i], current_date, older_than)
	# except KeyboardInterrupt:
	# 	print("Stopped early with %d videos" % len(dct))
	# except Exception as e:
	# 	print("Unexpected Error", e)
	# 	print("Stopped early with %d videos" % len(dct))
	# print("Done")
	# save_data(dct, "cnn_comments")

	dct = {}

	print(colored("\n=====",'green'))
	v_id = syntax_error_catch('Give a video id from the channel you\'d like to scrape (make sure you put it in' +
		' quotes! ex. "pFPd_Dhs51s"): ')
	c_id = get_channel_id_from_video_id(v_id)
	p_id = get_all_uploads_from_channel_id(c_id)
	v_ids = get_video_ids_from_playlist_id(p_id, 750)

	len_vids = len(v_ids)
	n = len(v_ids)
	assert n <= len_vids
	print(str(n) + " videos total")

	try:
		scraped_channels = load_data('scraped_channels')
	except Exception as e:
		scraped_channels = {}

	if c_id in scraped_channels.keys():
		save_name, last_video_id, dates = scraped_channels[c_id]
		dct = load_data(save_name)

		print(colored("\nWe've already partially scraped " + save_name + ".", 'yellow'))
		if last_video_id == 'COMPLETE':
			print(colored(save_name + " was already completely scraped, ending with videos released 2 weeks before " + str(dates[0])
				+ ", assuming no videos were deleted between then and " + str(dates[-1]) + "."), 'yellow')
			redo = syntax_error_catch('\nWould you like to rescrape this channel? Write "y" in quotes if you do: ')
			if redo != "y":
				print(colored("Exiting program.", 'yellow'))
				return
		print(colored("We'll be continuing to scrape " + save_name + " starting with the video after " + str(last_video_id) + ".", 'yellow'))
		index_of_last = v_ids.index(last_video_id)
		assert index_of_last < len_vids-1
		v_ids = v_ids[index_of_last + 1:]
		n = len(v_ids)

	else:
		category = syntax_error_catch('Tell us which MBFC category this channel falls under (cp, lb, lcb, q, rb) (make sure ' + 
			'you put it in quotes! ex. "cp"): ')
		save_name = category + "/" + syntax_error_catch('What\'s the name of the channel? We\'ll use this to save your data as /<category>/'
			+ '<input>_comments.pkl \n(Make sure you put it in quotes! ex. "dailymail"): ') + '_comments'

		last_video_id = None
		scraped_channels[c_id] = [save_name, last_video_id, []]

	try:
		for i in range(n):
			print("video %d out of %d: %s" % (i, n, v_ids[i]))
			add_response_to_dictionary(dct, v_ids[i], current_date, older_than)
			last_video_id = v_ids[i]
		last_video_id = 'COMPLETE'
	except KeyboardInterrupt:
		print("\nStopped early with %d videos" % len(dct))

	except Exception as e:
		print("\nUnexpected Error", e)
		print("Stopped early with %d videos" % len(dct))

	scraped_channels[c_id][1] = last_video_id
	try:
		scraped_channels[c_id][2].index(current_date[:10])
	except ValueError:
		scraped_channels[c_id][2] += [current_date[:10]]

	if dct:
		print(colored("Finished scraping up to video " + str(last_video_id) + ".", 'yellow'))
	else:
		print(colored("No videos available to scrape at this time.", 'yellow'))

	while True:
		try:
			save_data(dct, save_name)
		except IOError as e:
			trial = "data/" + category
			if not os.path.exists(trial):
				os.makedirs(trial)
		else:
			break
	print(colored("\nData saved to " + save_name + ". Exiting program!\n ===== \n", 'green'))
	save_data(scraped_channels, 'scraped_channels')


def syntax_error_catch(phrase):
	while True:
		try:
			answer = input(colored(phrase, 'yellow'))
		except (NameError, SyntaxError):
			print(colored("Please re-enter your answer in quotes.", 'red'))
		else:
			return answer

def save_data(obj, name):
    with open('data/'+ name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
        
def load_data(name):
    with open('data/' + name + '.pkl', 'rb') as f:
        return pickle.load(f)

WATCH_URL = "https://www.youtube.com/watch?v="
# The CLIENT_SECRETS_FILE variable specifies the name of a file that contains
# the OAuth 2.0 information for this application, including its client_id and
# client_secret.
CLIENT_SECRETS_FILE = "./client_secret.json"

# This OAuth 2.0 access scope allows for full read/write access to the
# authenticated user's account and requires requests to use an SSL connection.
SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'

def get_authenticated_service():
  flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
  credentials = flow.run_console()
  return build(API_SERVICE_NAME, API_VERSION, credentials = credentials)

client = get_authenticated_service()

# Build a resource based on a list of properties given as key-value pairs.
# Leave properties with empty values out of the inserted resource.
def build_resource(properties):
  resource = {}
  for p in properties:
    # Given a key like "snippet.title", split into "snippet" and "title", where
    # "snippet" will be an object and "title" will be a property in that object.
    prop_array = p.split('.')
    ref = resource
    for pa in range(0, len(prop_array)):
      is_array = False
      key = prop_array[pa]

      # For properties that have array values, convert a name like
      # "snippet.tags[]" to snippet.tags, and set a flag to handle
      # the value as an array.
      if key[-2:] == '[]':
        key = key[0:len(key)-2:]
        is_array = True

      if pa == (len(prop_array) - 1):
        # Leave properties without values out of inserted resource.
        if properties[p]:
          if is_array:
            ref[key] = properties[p].split(',')
          else:
            ref[key] = properties[p]
      elif key not in ref:
        # For example, the property is "snippet.title", but the resource does
        # not yet have a "snippet" object. Create the snippet object here.
        # Setting "ref = ref[key]" means that in the next time through the
        # "for pa in range ..." loop, we will be setting a property in the
        # resource's "snippet" object.
        ref[key] = {}
        ref = ref[key]
      else:
        # For example, the property is "snippet.description", and the resource
        # already has a "snippet" object.
        ref = ref[key]
  return resource

# Remove keyword arguments that are not set
def remove_empty_kwargs(**kwargs):
  good_kwargs = {}
  if kwargs is not None:
    for key, value in kwargs.items():
      if value:
        good_kwargs[key] = value
  return good_kwargs

# Retrieves all comment threads associated with a particular video.
# The request's videoId parameter identifies the video.
def comment_threads_list_by_video_id(client, **kwargs):
  kwargs = remove_empty_kwargs(**kwargs)
  response = client.commentThreads().list(**kwargs).execute()

  # Print response to terminal if desired
  # print_comments_response(response)
  return response

# This function prints the comments and their associated replies for a particular video's API response.
def print_comments_response(response):
	for comment_thread in response['items']:
		print("Original Comment:")
		print(comment_thread["snippet"]["topLevelComment"]["snippet"]["textDisplay"])
		for i in comment_thread['replies']['comments']:
			print("===")
			print(i["snippet"]["textDisplay"])
			# Prints the `i`-th reply to this comment
		print("=====")


# Returns a list of videos that match the API request parameters.
# If Video ID is specified, the list should have size 1, since IDs are unique.
# The current use of this API is as a way to get the video title and channel ID.
def videos_list_by_id(client, **kwargs):
  kwargs = remove_empty_kwargs(**kwargs)
  response = client.videos().list(**kwargs).execute()
  return response

# Returns list of replies to a specified comment.
# Pass the comment ID as a parameter named `parentId` in `kwargs`.
def comments_list(client, **kwargs):
  kwargs = remove_empty_kwargs(**kwargs)
  response = client.comments().list(**kwargs).execute()
  return response

# Given a channel ID, lists playlists from that channel.
# Pass the comment ID as a parameter named `channelId` in `kwargs`.
def playlists_list_by_channel_id(client, **kwargs):
	kwargs = remove_empty_kwargs(**kwargs)
	response = client.playlists().list(**kwargs).execute()
	return response

# Given a playlist ID from `kwargs`, return a response of the playlist contents (videos).
def playlist_items_list_by_playlist_id(client, **kwargs):
  kwargs = remove_empty_kwargs(**kwargs)
  response = client.playlistItems().list(**kwargs).execute()
  return response

# Helper function which, given a playlist ID, grabs video IDs of the videos inside.
def get_video_ids_from_playlist_id(p_id, maxVids=200):
	try:
		print("Attempting to pull Video IDs from the playlist with Playlist ID = " + str(p_id))
		response = playlist_items_list_by_playlist_id(client, part='contentDetails',
			maxResults=50, playlistId=p_id)
		# print(response)

	except Exception as e:
		print("Error with getting playlist items from Playlist ID %s" % p_id)
		print("Error details: ", e)

	v_ids = [i['contentDetails']['videoId'] for i in response['items']
		if 'contentDetails' in i and 'videoId' in i['contentDetails']]
	# Old attempts to do the same thing as the line above
	# for i in response['items']:
	# 	try:
	# 		v_ids.append(i['contentDetails']['videoId'])
	# 	except Exception as e:
	# 		print("Error getting video id from playlist items: ", e)

	while 'nextPageToken' in response and len(v_ids) < maxVids:
		try:
			response = playlist_items_list_by_playlist_id(client, part='contentDetails',
				pageToken=response['nextPageToken'], maxResults=50, playlistId=p_id)
			v_ids += [i['contentDetails']['videoId'] for i in response['items']
				if 'contentDetails' in i and 'videoId' in i['contentDetails']]
		except:
			response = {}
			print("Error with PageToken while getting Video IDs from Playlist ID %s" % p_id)
	return v_ids


# Given a video ID, returns the channel ID it came from.
def get_channel_id_from_video_id(v_id):
	response = videos_list_by_id(client, part='snippet', id=v_id)['items'][0]
	return response['snippet']['channelId']

# Given a Channel ID, returns a list of all uploads from that channel sorted by recency.
def get_all_uploads_from_channel_id(c_id):
	response = client.channels().list(part='contentDetails',id=c_id).execute()
	return response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

def get_videos_from_playlists_from_channel_id(dct, channel_id, max_vids=100):
	# Step 1: Get playlist IDs of playlists we want to scrape
	channel_response = playlists_list_by_channel_id(client, part='snippet', maxResults=50, channelId=channel_id)
	playlist_ids = [i['id'] for i in channel_response['items'] if 'id' in i]
	print(playlist_ids)

	# If there are more than 50 playlists, separate API requests are needed to read the next pages of results.
	# We add the rest of the pages of results iteratively.
	while 'nextPageToken' in channel_response:
		channel_response = playlists_list_by_channel_id(client, 
			pageToken=channel_response['nextPageToken'], part='snippet', maxResults=50, channelId=channel_id)
		playlist_ids += [i['id'] for i in channel_response['items'] if 'id' in i]
		print("Results did not fit in on the last page; used PageToken to get more playlists from the channel.")

	print("Number playlists scraped from the channel: ", len(playlist_ids))
	print("Here are the scraped playlist IDs:")
	print(playlist_ids)
	print("---")

	# Step 2: Get video IDs of all videos within those playlists.
	v_ids = []
	for p_id in playlist_ids:
		try:
			print("Scraping videos from playlist: %s" % p_id)
			print("")
			v_ids += get_video_ids_from_playlist_id(p_id)
			print("---")
			if len(v_ids) > max_vids:
				break
		except Exception as e:
			print("Error with getting video IDs from Playlist ID: %s. Error message: " % p_id, e)

	num_vids = len(v_ids)
	print("Number of videos accumulated from these playlists: ", num_vids)

	return v_ids

# The most important function of the script, which adds the comments and metadata
# of a video specified by Video ID to an input data dictionary. 
def add_response_to_dictionary(dct, v_id, date_scraped=None, older_than=None):
	try:
		comment_response = comment_threads_list_by_video_id(client, 
			part='snippet,replies', videoId=v_id, maxResults=100, order='relevance')
	except HttpError as e:
		print("HTTP Error when gathering comment threads. This video will be skipped: %s; " % v_id, e)
		return

	video_response = videos_list_by_id(client, part='snippet, statistics, contentDetails', id=v_id)
	video_title = video_response['items'][0]['snippet']['title']
	if video_title in dct:
		print("The video %s was already scraped, so it is being skipped." % v_id)
		return

	video_timestamp = video_response['items'][0]["snippet"]["publishedAt"]
	if older_than != None and video_timestamp > older_than:
		print("Skipping video %s because it is not old enough: %s" % (v_id, video_timestamp))
		return

	author = video_response['items'][0]["snippet"]['channelTitle']
	stats = video_response['items'][0]['statistics']

	duration = video_response['items'][0]['contentDetails']['duration']
	# if int(stats["commentCount"]) > 15000:
	# 	print("%s has more than 15000 comments+replies, so I will ignore it for now." % v_id)
	# (Such videos may take enormous time to scrape.)
	# 	return v_id
	viewCount = 0
	likeCount = 0
	dislikeCount = 0
	favoriteCount = 0
	
	stats_keys = stats.keys()
	if 'viewCount' in stats_keys:
		viewCount = stats["viewCount"]
	if 'likeCount' in stats_keys:
		likeCount = stats["likeCount"]
	if 'dislikeCount' in stats_keys:
		dislikeCount = stats['dislikeCount']
	if 'favoriteCount' in stats_keys:
		favoriteCount = stats['favoriteCount']			

	video_stats = (video_timestamp, author, duration, viewCount, likeCount,
		dislikeCount, favoriteCount, date_scraped)
	url = WATCH_URL + v_id

	video_comments = []

	# A list of (at most 100) comment threads sorted by relevance.
	items = comment_response['items']

	# If there are more than 100 comments, separate API requests are needed to read the next pages.
	# We have a function that adds the rest of the comment pages iteratively.
	if 'nextPageToken' in comment_response:
		page_token = comment_response['nextPageToken']
		items = iteratively_collect_comment_pages(items, v_id, page_token)

	print("Number of threads scraped: %d. Video upload date: %s" % (len(items), video_timestamp))
	num_comments_and_replies = len(items)
	# Iterate over the comments. Accumulate their replies and append them to `video_comments`.
	for comment_thread in items:
		author_name = comment_thread["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
		timestamp = comment_thread["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
		like_count = comment_thread["snippet"]["topLevelComment"]["snippet"]["likeCount"]

		metadata = (author_name, timestamp, like_count)
		comment_text = comment_thread["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
		comment_dictionary = {'original comment': [metadata, comment_text]}


		## For each comment, get the replies
		comment_id = comment_thread['id']
		reply_count = comment_thread["snippet"]['totalReplyCount']

		# A separate API request is needed to retrieve all replies.
		reply = comments_list(client, part='snippet', parentId=comment_id, maxResults=100)
		page_token = None
		if 'nextPageToken' in reply:
			page_token = reply['nextPageToken']
		reply = reply['items']

		# Iterate over all pages of results to get all replies to the comment. 
		replies = get_replies(comment_id, reply, reply_count, page_token)

		comment_dictionary['replies'] = replies
		video_comments.append(comment_dictionary)
		num_comments_and_replies += len(replies)

	# Ideally, the number of comments we scrape should be equal to the commentCount stat given in the video
	# but there are cases where the commentCount stat is more than what our script was able to access.
	print(num_comments_and_replies, stats["commentCount"])
	dct[video_title] = (url, video_comments, video_stats)

# Returns a concatenated list of all replies to a particular comment. 
def get_replies(comment_id, reply, reply_count, page_token=None):
	replies = []
	for r in reply:
		author_name = r["snippet"]["authorDisplayName"]
		timestamp = r["snippet"]["publishedAt"]
		like_count = r["snippet"]["likeCount"]
		
		metadata = (author_name, timestamp, like_count)
		comment_text = r["snippet"]["textDisplay"]
		replies.append([metadata, comment_text])

	# If there are more pages of results, get them
	if page_token != None:
		reply = comments_list(client, part='snippet', parentId=comment_id, pageToken=page_token, maxResults=100)
		for r in reply['items']:
			author_name = r["snippet"]["authorDisplayName"]
			timestamp = r["snippet"]["publishedAt"]
			like_count = r["snippet"]["likeCount"]
		
			metadata = (author_name, timestamp, like_count)
			comment_text = r["snippet"]["textDisplay"]
			replies.append([metadata, comment_text])

	# Iteratively get the rest of the pages
	while 'nextPageToken' in reply:
		page_token = reply['nextPageToken']
		reply = comments_list(client, part='snippet', parentId=comment_id, pageToken=page_token, maxResults=100)
		for r in reply['items']:
			author_name = r["snippet"]["authorDisplayName"]
			timestamp = r["snippet"]["publishedAt"]
			like_count = r["snippet"]["likeCount"]
		
			metadata = (author_name, timestamp, like_count)
			comment_text = r["snippet"]["textDisplay"]
			replies.append([metadata, comment_text])
	return replies

# Returns a concatenated list of all comments to a video.
def iteratively_collect_comment_pages(items, v_id, page_token):
	comment_response = comment_threads_list_by_video_id(client, 
		part='snippet', videoId=v_id, maxResults=100, pageToken=page_token, order='relevance')
	items += comment_response['items']
	# Read all pages.
	success = False
	while 'nextPageToken' in comment_response:
		page_token = comment_response['nextPageToken']
		success = False
		# Try 500 times. This function often fails transiently and just needs to be retried.
		max_retries = 1000
		max_results = 100
		for i in range(max_retries):
			try:
				comment_response = comment_threads_list_by_video_id(client, 
					part='snippet', videoId=v_id, maxResults=max_results, pageToken=page_token, order='relevance')
				items += comment_response['items']
				if i > 0:
					print("Successfully collected page after failure")
				success = True
				break
			except Exception as e:
				if i < 3 or i % 10 == 0:
					print("%d items collected so far. " % len(items)
						+ "Error collecting the next page of comments for Video ID " 
						+ "%s in attempt %d / %d. Retrying. Error Message:" % (v_id, i + 1, max_retries), e)

				# After 200 consecutive errors, stop trying to scrape up to 100 comments in one API call.
				if i > 200:
					max_results = 20
		if not success:
			print("Failed collecting pages 1000 times, so we continue with just %d items." % len(items))
			break
	return items

# Takes a saved comment data dictionary and returns a list of the video IDs
# of all videos inside.
def retrieveOldVideoIDs(dct):
	v_IDs = []
	for k, v in dct.items():
		if len(v[0]) < len(WATCH_URL):
			continue
		else:
			v_IDs.append(v[0][len(WATCH_URL):])
	return v_IDs

if __name__ == '__main__':
	run()
