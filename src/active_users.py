#script for extracting active user numbers from raw user sessions
import csv
import time
from datetime import datetime 
import operator

stickiness_threshold = 1.0/3

secs_per_day = 24*60*60
cohort_one_start = "11/1/2015"
cohort_one_end = "2/9/2016"
cohort_two_start = "2/10/2016"
cohort_two_end = "7/25/2016"
cohort_three_start = "7/26/2016"

with open('user_raw_sessions.csv') as csvfile:
	user_sessions_reader = csv.reader(csvfile, delimiter=',')
	user_logins = {}
	final_login_time = 0
	for i,row in enumerate(user_sessions_reader):
		if i > 0:
			user_id = row[2]
			if user_id != "":
				if user_id in user_logins:
					current_login_time = int(float(row[0]))
					if current_login_time - user_logins[user_id]["last_login_time"] > secs_per_day:
						user_logins[user_id]["days_with_login"] += 1
						user_logins[user_id]["last_login_time"] = current_login_time
				else:
					user_logins[row[2]] = {"first_login_time":int(float(row[0])),"last_login_time":int(float(row[0])),"days_with_login":0,}
			final_login_time = max(final_login_time,int(float(row[0])))


cohort_three_end = datetime.fromtimestamp(final_login_time).strftime('%m/%d/%Y') 

cohorts = {1:{"start":time.mktime(datetime.strptime(cohort_one_start, "%m/%d/%Y").timetuple()), "end":time.mktime(datetime.strptime(cohort_one_end, "%m/%d/%Y").timetuple()),"n":0,"av_stickiness":0,"n_active":0},2:{"start":time.mktime(datetime.strptime(cohort_two_start, "%m/%d/%Y").timetuple()),"end":time.mktime(datetime.strptime(cohort_two_end, "%m/%d/%Y").timetuple()),"n":0,"av_stickiness":0,"n_active":0},3:{"start":time.mktime(datetime.strptime(cohort_three_start, "%m/%d/%Y").timetuple()),"end":cohort_three_end,"n":0,"av_stickiness":0,"n_active":0}}

for key in user_logins:
	if user_logins[key]["first_login_time"] >= cohorts[2]["start"] and user_logins[key]["first_login_time"] < cohorts[3]["start"]:
		user_logins[key]["cohort"] = 2
	else:
		user_logins[key]["cohort"] = 3

stickiness = {}
for key in user_logins:
	delta = datetime.fromtimestamp(final_login_time) - datetime.fromtimestamp(user_logins[key]["first_login_time"])
	user_logins[key]["days_since_signup"] = delta.days
	if user_logins[key]["days_since_signup"] != 0:
		stickiness[key] = float(user_logins[key]["days_with_login"])/user_logins[key]["days_since_signup"]

for key in stickiness:
	cohorts[user_logins[key]["cohort"]]["av_stickiness"] += stickiness[key]
	cohorts[user_logins[key]["cohort"]]["n"] +=1
	if stickiness[key] >= stickiness_threshold:
		cohorts[user_logins[key]["cohort"]]["n_active"] += 1

for key in cohorts:
	if key != 1:
		cohorts[key]["av_stickiness"] = cohorts[key]["av_stickiness"]/cohorts[key]["n"]
		print cohorts[key]["av_stickiness"]
		print cohorts[key]["n_active"]

num_active_users = 0
for key in stickiness:
	if stickiness[key] > stickiness_threshold:
		num_active_users += 1

print num_active_users







		
