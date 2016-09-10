var async           = require("async"),
    fs              = require("fs"),
    child_process   = require("child_process"),
    csvWriter       = require('csv-write-stream');

var database        = require("/shared-code/database.js"),
    storage         = require("/shared-code/storage.js");


function writeCSVline(list, file)
{
    if(list.length == 0)
        return;
    var line = "";
    for(var i = 0; i < list.length; i ++)
    {
        line += list[i];
        if(i +1 < list.length )
            line += ",";
    }
    file.write(line)
}

var files_written = 0;
var timeseries_filename = "user_timeseries.csv";
var cohorts_filename = "user_cohorts.csv";

async.waterfall(
    [
        database.generateQueryFunction("SELECT data->>'user'as user, extract(epoch from time) as time, data->>'page'as page FROM Events WHERE event_type=1 AND ((data->'user') IS NOT NULL) ORDER BY time;",[]),
        function(results, callback)
        {
            if(results.rowCount < 1)
                callback()
            var session_tracker = {};
            var user_data = {};

            var close_function = function()
            {
                files_written++;
                if(files_written == 2)
                    callback();
            }

            var result = 
            {   
                "by_time": 
                    {
                        "id": [],
                        "end_time": [],
                        "n-sessions": [],
                        "sessions-with-match-request": [],
                        "avg-session-length": [],
                        "avg-session-interval": [],
                        "unique-users": []
                    }
            };

            var timeseries_file = fs.createWriteStream("/shared/"+timeseries_filename);
            var timeseries_writer = csvWriter();
            timeseries_writer.pipe(timeseries_file);
            timeseries_writer.on("finished", function(){timeseries_file.end();});
            timeseries_file.on("close", close_function);


            var stat_sampling_interval = 60*60*24; //get daily samples
            var sample_id = 0;
            var initial_time = (Math.floor(results.rows[0]["time"] /stat_sampling_interval))* stat_sampling_interval;
            var next_sampling_time = initial_time + stat_sampling_interval;

            var session_max_action_interval = 60*15;

            var current_sessions_list = [];
            var keep_sessions_window = 60*60*24*7; //keep last week of sessions;

            for(var i = 0; i < results.rowCount; ++i)
            {
                var next_entry = results.rows[i];

                while(next_entry["time"] >= next_sampling_time)
                {
                    //update sessions list: add tracked sessions + remove old ones
                    for(user in session_tracker)
                    {
                        if(session_tracker[user]["last"] + session_max_action_interval < next_sampling_time)
                        {
                            user_data[user]["last_session"] = session_tracker[user]["last"];

                            current_sessions_list.push(session_tracker[user]);
                            delete session_tracker[user];
                        }
                    }

                    for(var j = 0; j < current_sessions_list.length; ++j)
                    {
                        if(current_sessions_list[j]["last"] < next_sampling_time - keep_sessions_window)
                        {
                            current_sessions_list.splice(j,1);
                            j -= 1;
                        }
                    }

                    //create sample
                    var full_sample = 
                        {
                            "id": sample_id,
                            "end_time": next_sampling_time
                        };

                    full_sample["n-sessions"] = 0;
                    full_sample["sessions-with-match-request"] = 0;
                    full_sample["avg-session-length"] = 0;
                    full_sample["avg-session-interval"] = 0;

                    full_sample["unique-users"] = 0;

                    var group_by_user = {};
                    for(var j = 0; j < current_sessions_list.length; ++j)
                    {
                        if(! (current_sessions_list[j]["user"] in group_by_user))
                        {
                            group_by_user[current_sessions_list[j]["user"]] =
                                {
                                    "count": 0
                                };
                        }
                        group_by_user[current_sessions_list[j]["user"]]["count"] += 1;

                        full_sample["n-sessions"] ++;
                        if(current_sessions_list[j]["match_request"])
                            full_sample["sessions-with-match-request"] ++;

                        var session_length = current_sessions_list[j]["last"] - current_sessions_list[j]["first"];
                        full_sample["avg-session-length"] += (session_length-full_sample["avg-session-length"])  / full_sample["n-sessions"];
                        full_sample["avg-session-interval"] += (current_sessions_list[j]["time_since_last"]-full_sample["avg-session-interval"])  / full_sample["n-sessions"];
                    }

                    for(var user in group_by_user)
                        full_sample["unique-users"]++ ;

                    timeseries_writer.write(full_sample)

                    next_sampling_time += stat_sampling_interval;
                    sample_id++;
                }


                //add event
                var was_match_request = next_entry["page"].startsWith("/api/queue-matches") || next_entry["page"].startsWith("/api/retrieve");
                
                if(! (next_entry["user"] in user_data))
                    user_data[next_entry["user"]] = 
                        {
                            "signed_up": next_entry["time"],
                            "last_session": next_entry["time"]
                        };

                if(next_entry["user"] in session_tracker)
                {
                    if(next_entry["time"] - session_tracker[next_entry["user"]]["last"] > session_max_action_interval)
                    {
                        user_data[next_entry["user"]]["last_session"] = session_tracker[next_entry["user"]]["last"];

                        current_sessions_list.push(session_tracker[next_entry["user"]]);

                        session_tracker[next_entry["user"]]["first"] = next_entry["time"];
                        session_tracker[next_entry["user"]]["match_request"] = was_match_request;
                        session_tracker[next_entry["user"]]["time_since_last"] = next_entry["time"]-user_data[next_entry["user"]]["last_session"];
                    }

                    session_tracker[next_entry["user"]]["last"] = next_entry["time"];
                    session_tracker[next_entry["user"]]["match_request"] = session_tracker[next_entry["user"]]["match_request"] || was_match_request; 
                }
                else
                {
                    session_tracker[next_entry["user"]] = {
                        "first": next_entry["time"],
                        "last": next_entry["time"],
                        "user": next_entry["user"],
                        "match_request": was_match_request,
                        "time_since_last": (next_entry["time"]-user_data[next_entry["user"]]["last_session"]) 
                    };
                }
            }

            timeseries_writer.end();
            var final_sampling_time = next_sampling_time;


            var cohorts_file = fs.createWriteStream("/shared/"+cohorts_filename);
            var cohorts_writer = csvWriter();
            cohorts_writer.pipe(cohorts_file);
            cohorts_writer.on("finished", function(){cohorts_file.end();});
            cohorts_file.on("close", close_function);

            //do some user based analysis
            result["cohorts"] = 
            {
                "id": [],
                "start_time": [],
                "size": [],
                "long_term_returning": [],
                "final_rentention":[],
                "final_rentention_percent":[]
            };

            var cohort_length = 60*60*24*7;

            var current_cohort_finish = initial_time + cohort_length;
            var current_cohort_id = 0;

            var long_term_returning_requirement = 60*60*24*60;
            var final_retention_window = 60*60*24*7;
            while(current_cohort_finish < final_sampling_time)
            {
                var current_cohort_users = [];
                for(var user in user_data)
                {
                    if( user_data[user]["signed_up"] >= current_cohort_finish - cohort_length &&
                        user_data[user]["signed_up"] < current_cohort_finish)
                    {
                        var cohort_user = user_data[user];
                        cohort_user["user"] = user;
                        current_cohort_users.push(cohort_user);
                    }
                }

                var cohort_sample = 
                {
                    "id": current_cohort_id,
                    "start_time": current_cohort_finish - cohort_length,
                    "size": 0,
                    "long_term_returning": 0,
                    "final_rentention": 0,
                    "final_rentention_percent": 0
                }

                for(var i = 0; i < current_cohort_users.length; ++i)
                {
                    cohort_sample["size"]++;
                    if(current_cohort_users[i]["last_session"] - current_cohort_users[i]["signed_up"] > long_term_returning_requirement)
                       cohort_sample["long_term_returning"]++;
                   if(current_cohort_users[i]["last_session"] > final_sampling_time - final_retention_window)
                       cohort_sample["final_rentention"]++;
                }

                cohort_sample["final_rentention_percent"] = cohort_sample["size"]> 0 ? cohort_sample["final_rentention"] / cohort_sample["size"] : 0;

                cohorts_writer.write(cohort_sample);

                current_cohort_finish += stat_sampling_interval;
                current_cohort_id++;
            }

            cohorts_writer.end();
        },
        function(callback)
        {
            storage.store(timeseries_filename, callback);
        },
        function(file, callback)
        {
            storage.store(cohorts_filename, callback);
        }
    ],
    function(err, result)
    {
        console.log("finished", err, result);
    }
);