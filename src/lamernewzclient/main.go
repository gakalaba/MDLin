package main

import (
	"clients"
	"dlog"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"state"
	"time"
  "zipfgenerator"
)

var clientId *int = flag.Int(
	"clientId",
	0,
	"Client identifier for use in replication protocols.")

var conflicts *int = flag.Int(
	"conflicts",
	-1,
	"Percentage of conflicts. If < 0, a zipfian distribution will be used for "+
		"choosing keys.")

var conflictsDenom *int = flag.Int(
	"conflictsDenom",
	100,
	"Denominator of conflict fraction when conflicts >= 0.")

var cpuProfile *string = flag.String(
	"cpuProfile",
	"",
	"Name of file for CPU profile. If empty, no profile is created.")

var debug *bool = flag.Bool(
	"debug",
	true,
	"Enable debug output.")

var defaultReplicaOrder *bool = flag.Bool(
	"defaultReplicaOrder",
	false,
	"Use default replica order for Gryff coordination.")

var epaxosMode *bool = flag.Bool(
	"epaxosMode",
	false,
	"Run Gryff with same message pattern as EPaxos.")

var expLength *int = flag.Int(
	"expLength",
	30,
	"Length of the timed experiment (in seconds).")

var fastPaxos *bool = flag.Bool(
	"fastPaxos",
	false,
	"Send message directly to all replicas a la Fast Paxos.")

var forceLeader *int = flag.Int(
	"forceLeader",
	-1,
	"Replica ID to which leader-based operations will be sent. If < 0, an "+
		"appropriate leader is chosen by default.")

var coordinatorAddr *string = flag.String(
	"caddr",
	"",
	"Coordinator address.")

var coordinatorPort *int = flag.Int(
	"cport",
	7097,
	"Coordinator port.")

var maxProcessors *int = flag.Int(
	"maxProcessors",
	2,
	"GOMAXPROCS. Defaults to 2")

var numKeys *uint64 = flag.Uint64(
	"numKeys",
	10000,
	"Number of keys in simulated store.")

var proxy *bool = flag.Bool(
	"proxy",
	false,
	"Proxy writes at local replica.")

var rampDown *int = flag.Int(
	"rampDown",
	5,
	"Length of the cool-down period after statistics are measured (in seconds).")

var rampUp *int = flag.Int(
	"rampUp",
	5,
	"Length of the warm-up period before statistics are measured (in seconds).")

var randSleep *int = flag.Int(
	"randSleep",
	1,
	"Max number of milliseconds to sleep after operation completed.")

var randomLeader *bool = flag.Bool(
	"randomLeader",
	false,
	"Egalitarian (no leader).")

var reads *int = flag.Int(
	"reads",
	0,
	"Percentage of reads.")

var regular *bool = flag.Bool(
	"regular",
	false,
	"Perform operations with regular consistency. (only for applicable protocols)")

var replProtocol *string = flag.String(
	"replProtocol",
	"",
	"Replication protocol used by clients and servers.")

var rmws *int = flag.Int(
	"rmws",
	0,
	"Percentage of rmws.")

var sequential *bool = flag.Bool(
	"sequential",
	true,
	"Perform operations with sequential consistency. "+
		"(only for applicable protocols")

var statsFile *string = flag.String(
	"statsFile",
	"",
	"Export location for collected statistics. If empty, no file file is written.")

var fanout *int = flag.Int(
	"fanout",
	1,
	"Fanout. Defaults to 1.")

var singleShardAware *bool = flag.Bool(
	"SSA",
	false,
	"Single shard awareness optimization. Defaults to false.")

var thrifty *bool = flag.Bool(
	"thrifty",
	false,
	"Only initially send messages to nearest quorum of replicas.")

var writes *int = flag.Int(
	"writes",
	1000,
	"Percentage of updates (writes).")

var zipfS = flag.Float64(
	"zipfS",
	2,
	"Zipfian s parameter. Generates values k∈ [0, numKeys] such that P(k) is "+
		"proportional to (v + k) ** (-s)")

var zipfV = flag.Float64(
	"zipfV",
	1,
	"Zipfian v parameter. Generates values k∈ [0, numKeys] such that P(k) is "+
		"proportional to (v + k) ** (-s)")

func createClient() clients.Client {
	switch *replProtocol {
	case "abd":
		return clients.NewAbdClient(int32(*clientId), *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, *regular)
	case "gryff":
		return clients.NewGryffClient(int32(*clientId), *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, *regular, *sequential, *proxy, *thrifty, *defaultReplicaOrder,
			*epaxosMode)
	case "epaxos":
		return clients.NewProposeClient(int32(*clientId), *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, false, true)
	case "mdl":
		return clients.NewMDLClient(int32(*clientId), *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, false, true, *singleShardAware)
	case "ss-mdl":
		return clients.NewSSMDLClient(int32(*clientId), *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, false, false)
	default:
		return clients.NewProposeClient(int32(*clientId), *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, false, false)
	}
}

func Max(a int64, b int64) int64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func main() {
	flag.Parse()


	dlog.DLOG = *debug

	runtime.GOMAXPROCS(2)

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatalf("Error creating CPU profile file %s: %v\n", *cpuProfile, err)
		}
		pprof.StartCPUProfile(f)
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt)
		go catchKill(interrupt)
		defer pprof.StopCPUProfile()
	}

	client := createClient()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
  zipf, err := zipfgenerator.NewZipfGenerator(r, 0, *numKeys, *zipfS, false)
  if err != nil {
    panic("problem making the zipfian generator :0")
  }
	var count int32
	count = 0

	go func(client clients.Client) {
		time.Sleep(time.Duration(*expLength+1) * time.Second)
		client.Finish()
	}(client)

	// Add these keys in the store 
	global_timeline := int64(zipf.Uint64())
	next_post_id := int64(zipf.Uint64())
	next_user_id := int64(zipf.Uint64())
	auths := int64(zipf.Uint64())
	auth := int64(zipf.Uint64())
	users := int64(zipf.Uint64())
	users_by_time := int64(zipf.Uint64())
	ops := []state.Operation{state.PUT, state.PUT, state.PUT, state.PUT, state.PUT, state.PUT, state.PUT}
	appState := []int64{global_timeline, next_post_id, next_user_id, auths, auth, users, users_by_time}
	client.AppRequest(ops, appState)
  post_id := int64(0)



  lamernewzTypes := [12]string{"Login", "Logout", "ResetPassword", "CreateAccount", "UpdateProfile", "InsertNews", "EditNews", "DeleteNews", "VoteNews", "PostComment", "VoteComment", "GetNews", "GetComments"}
	selector := 0

	start := time.Now()
	now := start
	currRuntime := now.Sub(start)
	for int(currRuntime.Seconds()) < *expLength {
		if *randSleep > 0 {
			time.Sleep(time.Duration(r.Intn(*randSleep * 1e6))) // randSleep ms
		}

		opType := r.Uint64() % 100
		//dlog.Printf("Client %v about to issue AppRequest at time %v\n", *clientId, time.Now().UnixMilli())
		before := time.Now()
		if (opType < 2) {
			// Login 2%
			selector = 0
			LoginTransformed(users, auth, client, zipf)
		} else if (opType < 4) {
			// Logout 2%
			selector = 1
			LogoutTransformed(auths, client, zipf)
		} else if (opType < 5) {
			// ResetPassword 1%
			selector = 2
			ResetPasswordTransformed(users, users_by_time, next_user_id, auths, auth, client, zipf)
		} else if (opType < 6) {
			// CreateAccount 1%
			selector = 3
			CreateAccountTransformed(post_id, global_timeline, next_post_id, client, zipf)
		} else if (opType < 8) {
			// UpdateProfile 2%
			selector = 4
			ShowTimelineTransformed(users_by_time, global_timeline, client, zipf)
		} else if (opType < 14) {
			// InsertNews 6%
			selector = 5
			FollowTransformed(auths, client, zipf)
		} else if (opType < 18) {
			// EditNews 4%
			selector = 6
			EditNews()
		} else if (opType < 20) {
			// DeleteNews 2%
			selector = 7
			ShowTimelineTransformed(users_by_time, global_timeline, client, zipf)
		} else if (opType < 35) {
			// VoteNews 15%
			selector = 8
			ShowTimelineTransformed(users_by_time, global_timeline, client, zipf)
		} else if (opType < 40) {
			// PostComment 5%
			selector = 9
			ShowTimelineTransformed(users_by_time, global_timeline, client, zipf)
		} else if (opType < 50) {
			// VoteComment 10%
			selector = 10
			ShowTimelineTransformed(users_by_time, global_timeline, client, zipf)
		} else if (opType < 75) {
			// GetNews 25%
			selector = 11
			ShowTimelineTransformed(users_by_time, global_timeline, client, zipf)
		} else {
			// GetComments 25%
			selector = 12
			ProfileTransformed(users, auths, global_timeline, client, zipf)
		}

		after := time.Now()
		post_id++
                //dlog.Printf("!!!!POST took %d microseconds\n", int64(after.Sub(before).Microseconds()))

		opString := "app"
		lamernewzType := lamernewzTypes[selector]
		count++
		dlog.Printf("AppRequests attempted: %d\n", count)
		//dlog.Printf("AppRequests attempted: %d at time %d\n", count, time.Now().UnixMilli())

		currInt := int(currRuntime.Seconds())
		if *rampUp <= currInt && currInt < *expLength-*rampDown {
			lat := int64(after.Sub(before).Nanoseconds())
			fmt.Printf("%s,%d,%d,%d\n", opString, lat, 0, count)
			fmt.Printf("%s,%d,%d,%d\n", lamernewzType, lat, 0, count)

		}
		now = time.Now()
		currRuntime = now.Sub(start)
	}
	log.Printf("Total AppRequests attempted: %d, total system level requests: %d\n", count, count*int32(*fanout))
	log.Printf("Experiment over after %f seconds\n", currRuntime.Seconds())
	client.Finish()
}



//***********************************************************//
//******************* Lamernewz App *************************//
//***********************************************************//
// Based on https://github.com/antirez/lamernews/blob/master/app.rb

//***********************************************************//
//******************** Lamernewz Login **********************//
//***********************************************************//
// Based on https://github.com/antirez/lamernews/blob/d08bf6baa81216805561f3e5500e43a9dc32c7df/app.rb#L664

/*
get '/api/login' ("username", "password") do
  id = $r.get("username.to.id:#{username.downcase}")
  if !id {
    return nil
  }
  $r.hgetall("user:#{id}")
end
*/

func LoginSequential(username int64, client clients.Client, zipf *zipfgenerator.ZipfGenerator) {
	//r := rand.New(rand.NewSource(time.Now().UnixNano()))
	client.AppRequest([]state.Operation{state.GET}, []int64{username})
	user_id = int64(zipf.Uint64())
  client.AppRequest([]state.Operation{state.GET}, []int64{user_id})
}

func LoginTransformed(username int64, client clients.Client, zipf *zipfgenerator.ZipfGenerator) {
	//r := rand.New(rand.NewSource(time.Now().UnixNano()))
	client.AppRequest([]state.Operation{state.GET}, []int64{username})
	user_id = int64(zipf.Uint64())
  client.AppRequest([]state.Operation{state.GET}, []int64{user_id})
}

//***********************************************************//
//********************* Lamernewz Logout ********************//
//***********************************************************//
// Based on https://github.com/antirez/lamernews/blob/d08bf6baa81216805561f3e5500e43a9dc32c7df/app.rb#L651C1-L651C22
/*
post '/api/logout' ($user) do
  $r.del("auth:#{user['auth']}")
  new_auth_token = get_rand
  $r.hmset("user:#{user['id']}","auth",new_auth_token)
  $r.set("auth:#{new_auth_token}",user['id'])
end
*/
func LogoutSequential(client clients.Client, zipf *zipfgenerator.ZipfGenerator) {
	authuser := int64(zipf.Uint64())
	user := int64(zipf.Uint64())
	authnewauthtoken := int64(zipf.Uint64())
  client.AppRequest([]state.Operation{state.PUT}, []int64{authuser})
	client.AppRequest([]state.Operation{state.PUT}, []int64{user})
	client.AppRequest([]state.Operation{state.PUT}, []int64{authnewauthtoken})
}

func LogoutTransformed(client clients.Client, zipf *zipfgenerator.ZipfGenerator) {
	authuser := int64(zipf.Uint64())
	user := int64(zipf.Uint64())
	authnewauthtoken := int64(zipf.Uint64())
  client.AppRequest([]state.Operation{state.PUT, state.PUT, state.PUT}, []int64{authuser, user, authnewauthtoken})
}

//***********************************************************//
//**************** Lamernewz ResetPassword ******************//
//***********************************************************//
// Based on https://github.com/antirez/lamernews/blob/d08bf6baa81216805561f3e5500e43a9dc32c7df/app.rb#L688
/*
get '/api/reset-password' ("username", "email") do
  id = $r.get("username.to.id:#{username.downcase}")
  if !id {
    return nil
  }
  $r.hgetall("user:#{id}")

  if cond(user) {
    $r.hset("user:#{id}","pwd_reset",Time.now.to_i)
  }
end
*/
func ResetPasswordSequential(username int64, client clients.Client,
				zipf *zipfgenerator.ZipfGenerator) {
  client.AppRequest([]state.Operation{state.GET}, []int64{username})
	user_id = int64(zipf.Uint64())
  client.AppRequest([]state.Operation{state.GET}, []int64{user_id})
  client.AppRequest([]state.Operation{state.GET}, []int64{user_id})
}

func ResetPasswordTransformed(username int64, client clients.Client,
				zipf *zipfgenerator.ZipfGenerator) {
  client.AppRequest([]state.Operation{state.GET}, []int64{username})
	user_id = int64(zipf.Uint64())
  client.AppRequest([]state.Operation{state.GET}, []int64{user_id})
  client.AppRequest([]state.Operation{state.GET}, []int64{user_id})
}

//***********************************************************//
//*************** Lamernewz CreateAccount *******************//
//***********************************************************//
// Based on https://github.com/antirez/lamernews/blob/d08bf6baa81216805561f3e5500e43a9dc32c7df/app.rb#L729
/*
post '/api/create_account' ("username", "password") do
  if $r.exists("username.to.id:#{username.downcase}") {
    return
  }
  key = "limit:tags"
  if $r.exists(key) {
    return
  }
  $r.setex(key,delay,1)

  id = $r.incr("users.count")
  auth_token = get_rand
  apisecret = get_rand
  salt = get_rand
  $r.hmset("user:#{id}",
        "id",id,
        "username",username,
        "salt",salt,
        "password",hash_password(password,salt),
        "ctime",Time.now.to_i,
        "karma",UserInitialKarma,
        "about","",
        "email","",
        "auth",auth_token,
        "apisecret",apisecret,
        "flags","",
        "karma_incr_time",Time.new.to_i)
  $r.set("username.to.id:#{username.downcase}",id)
  $r.set("auth:#{auth_token}",id)
  if id.to_i == 1 {
    $r.hmset("user:#{id}","flags","a")
  }
end
*/

func CreateAccountSequential(username int64, client clients.Client, zipf *zipfgenerator.ZipfGenerator) {
	client.AppRequest([]state.Operation{state.GET}, []int64{username})
  key := int64(zipf.Uint64())
	client.AppRequest([]state.Operation{state.GET}, []int64{key})
	client.AppRequest([]state.Operation{state.PUT}, []int64{key})

  userscount := int64(zipf.Uint64())
	client.AppRequest([]state.Operation{state.CAS}, []int64{userscount})
  id := int64(zipf.Uint64())
	client.AppRequest([]state.Operation{state.PUT}, []int64{id})

	client.AppRequest([]state.Operation{state.PUT}, []int64{username})
  auth := int64(zipf.Uint64())
	client.AppRequest([]state.Operation{state.PUT}, []int64{auth})
	client.AppRequest([]state.Operation{state.PUT}, []int64{id})
}

func CreateAccountTransformed(username int64, client clients.Client, zipf *zipfgenerator.ZipfGenerator) {
	client.AppRequest([]state.Operation{state.GET}, []int64{username})
  key := int64(zipf.Uint64())
	client.AppRequest([]state.Operation{state.GET}, []int64{key})

  userscount := int64(zipf.Uint64())
  client.AppRequest([]state.Operation{state.PUT, state.CAS}, []int64{key, userscount})
  id := int64(zipf.Uint64())

  auth := int64(zipf.Uint64())
  client.AppRequest([]state.Operation{state.PUT, state.PUT, state.PUT}, []int64{id, username, auth})

	client.AppRequest([]state.Operation{state.PUT}, []int64{id})
}

//***********************************************************//
//****************** Lamernewz InsertNews *******************//
//***********************************************************//
// Based on https://github.com/antirez/lamernews/blob/d08bf6baa81216805561f3e5500e43a9dc32c7df/app.rb#L787
/*
post '/api/submit' ("title","news_id",:url,:text) do
  if $r.ttl("user:#{$user['id']}:submitted_recently") > 0 {
    return
  }
  if $r.get("url:"+url)) {
    return
  }
  news_id = $r.incr("news.count")
  $r.hmset("news:#{news_id}",
        "id", news_id,
        "title", title,
        "url", url,
        "user_id", user_id,
        "ctime", ctime,
        "score", 0,
        "rank", 0,
        "up", 0,
        "down", 0,
        "comments", 0)
  vote_news TDOODODO
  $r.zadd("user.posted:#{user_id}",ctime,news_id)
  $r.zadd("news.cron",ctime,news_id)
  $r.zadd("news.top",rank,news_id)
  $r.setex("url:"+url,PreventRepostTime,news_id) if !textpost
  $r.setex("user:#{$user['id']}:submitted_recently",NewsSubmissionBreak,'1')

end
*/

func InsertNewsSequential(user_id int64, url int64,
    client clients.Client, zipf *zipfgenerator.ZipfGenerator) {

  submitted_recently := int64(zipf.Uint64())
	client.AppRequest([]state.Operation{state.GET}, []int64{submitted_recently})

  client.AppRequest([]state.Operation{state.GET}, []int64{url})

  client.AppRequest([]state.Operation{state.PUT}, []int64{NewsCount})

  news_id := int64(zipf.Uint64())
  client.AppRequest([]state.Operation{state.PUT}, []int64{news_id})

  // news_id int64, user_id int64, vote_type int64
  vote_type := int64(zipf.Uint64())
  VoteNewsSequential(news_id, user_id, vote_type)

  user_posted := int64(zipf.Uint64())
  client.AppRequest([]state.Operation{state.PUT}, []int64{user_posted})
  client.AppRequest([]state.Operation{state.PUT}, []int64{NewsCron})
  client.AppRequest([]state.Operation{state.PUT}, []int64{NewsTop})
  client.AppRequest([]state.Operation{state.PUT}, []int64{url})
  client.AppRequest([]state.Operation{state.PUT}, []int64{user_id})
}

func InsertNewsTransformed(user_id int64, url int64,
    client clients.Client, zipf *zipfgenerator.ZipfGenerator) {

  submitted_recently := int64(zipf.Uint64())
	client.AppRequest([]state.Operation{state.GET}, []int64{submitted_recently})

  client.AppRequest([]state.Operation{state.GET}, []int64{url})

  client.AppRequest([]state.Operation{state.PUT}, []int64{NewsCount})

  // news_id int64, user_id int64, vote_type int64
  news_id := int64(zipf.Uint64())
  vote_type := int64(zipf.Uint64())
  user_posted := int64(zipf.Uint64())
  VoteNewsTransformed(news_id, user_id, vote_type, client, zipf, []state.Operation{state.PUT}, []int64{news_id},
                                                     []state.Operation{state.PUT, state.PUT}, []int64{user_posted, NewsCron})

  client.AppRequest([]state.Operation{state.PUT, state.PUT, state.PUT}, []int64{NewsTop, url, user_id})
}

//***********************************************************//
//******************* Lamernewz EditNews ********************//
//***********************************************************//
// Based on https://github.com/antirez/lamernews/blob/d08bf6baa81216805561f3e5500e43a9dc32c7df/app.rb#L798

/*
post '/api/submit' ("title","news_id",:url,:text) do
  //news = get_news_by_id(news_id)
    news = $r.hgetall("news:#{nid}")
    if !news {
      return []
    }
    $r.hmset("news:#{news["id"]}","rank",real_rank)
    $r.zadd("news.top",real_rank,news["id"])
    $r.hget("user:#{news["user_id"]}","username")

    if $user { //$user is a global variable!
      $r.zscore("news.up:#{n["id"]}",$user["id"])
      $r.zscore("news.down:#{n["id"]}",$user["id"])
    }
  //
  if cond(news) {  
    if $r.get("url:"+url) {
      return false
    }
    $r.del("url:"+news['url'])
    $r.setex("url:"+url,PreventRepostTime,news_id) if !textpost
  }

  $r.hmset("news:#{news_id}",
        "title", title,
        "url", url)

end
*/

func EditNewsSequential(news_id int64, user_id int64, url int64,
		client clients.Client, zipf *zipfgenerator.ZipfGenerator) {
  get_news_by_idSequential(news_id, user_id, false, client, zipf)
  client.AppRequest([]state.Operation{state.GET}, []int64{url})
  oldurl := int64(zipf.Uint64())
  client.AppRequest([]state.Operation{state.PUT}, []int64{oldurl})
  client.AppRequest([]state.Operation{state.PUT}, []int64{url})
  client.AppRequest([]state.Operation{state.PUT}, []int64{news_id})
}

func EditNewsTransformed(news_id int64, user_id int64, url int64,
		client clients.Client, zipf *zipfgenerator.ZipfGenerator) {
  get_news_by_idTransformed(news_id, user_id, false, client, zipf, nil, nil, nil, nil)
  client.AppRequest([]state.Operation{state.GET}, []int64{url})
  oldurl := int64(zipf.Uint64())
  client.AppRequest([]state.Operation{state.PUT, state.PUT, state.PUT}, []int64{oldurl, url, news_id})
}

//************************************************************//
//****************** Lamernewz DeleteNews ********************//
//************************************************************//
// Based on https://github.com/antirez/lamernews/blob/d08bf6baa81216805561f3e5500e43a9dc32c7df/app.rb#L814
/*
post '/api/delnews' (news_id, user_id) do
  //news = get_news_by_id(news_id)
    news = $r.hgetall("news:#{nid}")
    if !news {
      return []
    }
    $r.hmset("news:#{news["id"]}","rank",real_rank)
    $r.zadd("news.top",real_rank,news["id"])
    $r.hget("user:#{news["user_id"]}","username")

    if $user { //$user is a global variable!
      $r.zscore("news.up:#{n["id"]}",$user["id"])
      $r.zscore("news.down:#{n["id"]}",$user["id"])
    }
  //
  $r.hmset("news:#{news_id}","del",1)
  $r.zrem("news.top",news_id)
  $r.zrem("news.cron",news_id)
end
*/

func DeleteNewsSequential(user_id int64, news_id int64,
		client clients.Client, zipf *zipfgenerator.ZipfGenerator) {

	get_news_by_idSequential(news_id, user_id, false, client, zipf)
  client.AppRequest([]state.Operation{state.PUT}, []int64{news_id})
  client.AppRequest([]state.Operation{state.PUT}, []int64{NewsTop})
  client.AppRequest([]state.Operation{state.PUT}, []int64{NewsCron})
}

func DeleteNewsSequential(user_id int64, news_id int64,
		client clients.Client, zipf *zipfgenerator.ZipfGenerator) {

	get_news_by_idTransformed(news_id, user_id, false, client, zipf, nil, nil, nil, nil)
  client.AppRequest([]state.Operation{state.PUT, state.PUT, state.PUT}, []int64{news_id, NewsTop, NewsCron})
}

//************************************************************//
//******************* Lamernewz VoteNews *********************//
//************************************************************//
// https://github.com/antirez/lamernews/blob/d08bf6baa81216805561f3e5500e43a9dc32c7df/app.rb#L832
/*
post '/api/votenews' ("news_id","vote_type") do
  user = r.hgetall("user:#{user_id}")
  //news = get_news_by_id(news_id)
    news = $r.hgetall("news:#{nid}")
    if !news {
      return []
    }
    $r.hmset("news:#{news["id"]}","rank",real_rank)
    $r.zadd("news.top",real_rank,news["id"])
    $r.hget("user:#{news["user_id"]}","username")

    if $user { //$user is a global variable!
      $r.zscore("news.up:#{n["id"]}",$user["id"])
      $r.zscore("news.down:#{n["id"]}",$user["id"])
    }
  //
  if !news || !user {
    return false
  }
  if $r.zscore("news.up:#{news_id}",user_id) or
       $r.zscore("news.down:#{news_id}",user_id) {
         return
  }
  
  karma = $r.hget("user:#{user_id}","karma")
  if karma > val {
    return false
  }
  if $r.zadd("news.#{vote_type}:#{news_id}", Time.now.to_i, user_id) {
    $r.hincrby("news:#{news_id}",vote_type,1)
  }
  if vote_type == up {
    $r.zadd("user.saved:#{user_id}", Time.now.to_i, news_id)
  }
  // score = compute news()
    upvotes = $r.zrange("news.up:#{news["id"]}",0,-1,:withscores => true)
    downvotes = $r.zrange("news.down:#{news["id"]}",0,-1,:withscores => true)
  //
  news["score"] = score
  rank = compute_news_rank(news)
  $r.hmset("news:#{news_id}",
        "score",score,
        "rank",rank)
  $r.zadd("news.top",rank,news_id)

  userkey = "user:#{user_id}"
  $r.hincrby(userkey,"karma",increment)
end
*/

func VoteNewsSequential(news_id int64, user_id int64, vote_type int64,
		client clients.Client, zipf *zipfgenerator.ZipfGenerator) {

  client.AppRequest([]state.Operation{state.GET}, []int64{user_id})
  keys := get_news_by_idSequential(news_id, user_id, false, client, zipf)

	newsup := keys[0]
	newsdown := keys[1]
  client.AppRequest([]state.Operation{state.GET}, []int64{newsup})
  client.AppRequest([]state.Operation{state.GET}, []int64{newsdown})

  client.AppRequest([]state.Operation{state.GET}, []int64{user_id})

  client.AppRequest([]state.Operation{state.PUT}, []int64{vote_type})
  client.AppRequest([]state.Operation{state.PUT}, []int64{news_id})

  saveduser := int64(zipf.Uint64())
  client.AppRequest([]state.Operation{state.PUT}, []int64{saveduser})

  client.AppRequest([]state.Operation{state.GET}, []int64{newsup})
  client.AppRequest([]state.Operation{state.GET}, []int64{newsdown})

  client.AppRequest([]state.Operation{state.PUT}, []int64{news_id})
  client.AppRequest([]state.Operation{state.PUT}, []int64{NewsTop})

  client.AppRequest([]state.Operation{state.PUT}, []int64{user_id})
}

func VoteNewsTransformed(news_id int64, user_id int64, vote_type int64,
		client clients.Client, zipf *zipfgenerator.ZipfGenerator,
    opsbefore []state.Operation, keysbefore []int64,
    opsafter []state.Operation, keysafter []int64) {

  // Passing in first op into get_news_by_id, to group them together
  keys := get_news_by_idTransformed(news_id, user_id, false, client, zipf,
            append(opsbefore, []state.Operation{state.GET}),
            append(keysbefore, []int64{user_id}),
            nil, nil)

	newsup := keys[0]
	newsdown := keys[1]
  client.AppRequest([]state.Operation{state.GET, state.GET}, []int64{newsup, newsdown})

  client.AppRequest([]state.Operation{state.GET}, []int64{user_id})

  client.AppRequest([]state.Operation{state.PUT}, []int64{vote_type})
  saveduser := int64(zipf.Uint64())
  client.AppRequest([]state.Operation{state.PUT, state.PUT, state.GET, state.GET}, []int64{news_id, saveduser, newsup, newsdown})

  client.AppRequest(append([]state.Operation{state.PUT, state.PUT, state.PUT}, opsafter),
                              append([]int64{news_id, NewsTop, user_id}, keysafter))
}

//************************************************************//
//***************** Lamernewz PostComment ********************//
//************************************************************//
// Based on https://github.com/antirez/lamernews/blob/d08bf6baa81216805561f3e5500e43a9dc32c7df/app.rb#L857
/*
post '/api/postcomment' ("news_id","comment_id","parent_id",:comment) do
  //news = get_news_by_id(news_id)
    news = $r.hgetall("news:#{nid}")
    if !news {
      return []
    }
    $r.hmset("news:#{news["id"]}","rank",real_rank)
    $r.zadd("news.top",real_rank,news["id"])
    $r.hget("user:#{news["user_id"]}","username")

    if $user { //$user is a global variable!
      $r.zscore("news.up:#{n["id"]}",$user["id"])
      $r.zscore("news.down:#{n["id"]}",$user["id"])
    }
  //
  if !news {
    return false
  }

  if r.hget(key,comment_id) {
    return false
  }

  // commend_id = insert
    if comment['parent_id'] != -1 {
      if !r.hget(key,comment['parent_id']) {
        return false
      }
      id = r.hincrby(key,:nextid,1)
      r.hset(key,id,comment.to_json)
    }
  //

  if !comment_id {
    return false
  }

  $r.hincrby("news:#{news_id}","comments",1);
  $r.zadd("user.comments:#{user_id}",
            Time.now.to_i,
            news_id.to_s+"-"+comment_id.to_s);
  if $r.exists("user:#{p['user_id']}") {
    $r.hincrby("user:#{p['user_id']}","replies",1)
  }
end
*/

func PostCommentSequential(news_id int64, user_id int64,
		client clients.Client, zipf *zipfgenerator.ZipfGenerator) {

  get_news_by_idSequential(news_id, user_id, false, client, zipf)

  threadkey := int64(zipf.Uint64())
	client.AppRequest([]state.Operation{state.GET}, []int64{threadkey})

  client.AppRequest([]state.Operation{state.GET}, []int64{threadkey})
	client.AppRequest([]state.Operation{state.PUT}, []int64{threadkey})
	client.AppRequest([]state.Operation{state.PUT}, []int64{threadkey})

  client.AppRequest([]state.Operation{state.PUT}, []int64{news_id})
  user_comments := int64(zipf.Uint64())
	client.AppRequest([]state.Operation{state.PUT}, []int64{user_comments})
	client.AppRequest([]state.Operation{state.GET}, []int64{user_id})
	client.AppRequest([]state.Operation{state.PUT}, []int64{user_id})
}

func PostCommentTransformed(news_id int64, user_id int64,
		client clients.Client, zipf *zipfgenerator.ZipfGenerator) {

  get_news_by_idTransformed(news_id, user_id, false, client, zipf, nil, nil, nil, nil)

  threadkey := int64(zipf.Uint64())
	client.AppRequest([]state.Operation{state.GET}, []int64{threadkey})

  client.AppRequest([]state.Operation{state.GET}, []int64{threadkey})
	client.AppRequest([]state.Operation{state.PUT}, []int64{threadkey})
	client.AppRequest([]state.Operation{state.PUT}, []int64{threadkey})

  user_comments := int64(zipf.Uint64())
  client.AppRequest([]state.Operation{state.PUT, state.PUT, state.GET, state.PUT}, []int64{news_id, user_comments, user_id, user_id})
}


//************************************************************//
//***************** Lamernewz DeleteComment ********************//
//************************************************************//
// Based on https://github.com/antirez/lamernews/blob/d08bf6baa81216805561f3e5500e43a9dc32c7df/app.rb#L1858
/*
post '/api/postcomment' ("news_id","comment_id","parent_id",:comment) do
  //news = get_news_by_id(news_id)
    news = $r.hgetall("news:#{nid}")
    if !news {
      return []
    }
    $r.hmset("news:#{news["id"]}","rank",real_rank)
    $r.zadd("news.top",real_rank,news["id"])
    $r.hget("user:#{news["user_id"]}","username")

    if $user { //$user is a global variable!
      $r.zscore("news.up:#{n["id"]}",$user["id"])
      $r.zscore("news.down:#{n["id"]}",$user["id"])
    }
  //
  if !news {
    return false
  }

  c = r.hget(key, comment_id)
  if !c or !(c['ctime']) {
    return false
  }

  old = r.hget(key,comment_id)
  if !old {
    return false
  }
  comment = merge(old, updates)
  r.hset(key,comment_id,comment)

  $r.hincrby("news:#{news_id}","comments",-1);
end
*/

func DeleteCommentSequential(user_id int64, news_id int64,
		client clients.Client, zipf *zipfgenerator.ZipfGenerator) {

  get_news_by_idSequential(news_id, user_id, false, client, zipf)

  threadkey := int64(zipf.Uint64())
	client.AppRequest([]state.Operation{state.GET}, []int64{threadkey})

  client.AppRequest([]state.Operation{state.GET}, []int64{threadkey})
	client.AppRequest([]state.Operation{state.PUT}, []int64{threadkey})

  client.AppRequest([]state.Operation{state.PUT}, []int64{news_id})
}

func DeleteCommentSequential(user_id int64, news_id int64,
		client clients.Client, zipf *zipfgenerator.ZipfGenerator) {

  get_news_by_idTransformed(news_id, user_id, false, client, zipf, nil, nil, nil, nil)

  threadkey := int64(zipf.Uint64())
	client.AppRequest([]state.Operation{state.GET}, []int64{threadkey})

  client.AppRequest([]state.Operation{state.GET}, []int64{threadkey})
	client.AppRequest([]state.Operation{state.PUT}, []int64{threadkey})

  client.AppRequest([]state.Operation{state.PUT}, []int64{news_id})
}

//************************************************************//
//***************** Lamernewz UpdateProfile ******************//
//************************************************************//
// Based on https://github.com/antirez/lamernews/blob/d08bf6baa81216805561f3e5500e43a9dc32c7df/app.rb#L887
/*
post '/api/updateprofile' (:about, :email, :password) do
  if cond(password) {
    $r.hmset("user:#{$user['id']}","password",
            hash_password(params[:password],$user['salt']))
  }
  $r.hmset("user:#{$user['id']}",
        "about", params[:about][0..4095],
        "email", params[:email][0..255])
end
*/

func UpdateProfileSequential(user_id int64,
		client clients.Client, zipf *zipfgenerator.ZipfGenerator) {

	client.AppRequest([]state.Operation{state.PUT}, []int64{user_id})
	client.AppRequest([]state.Operation{state.PUT}, []int64{user_id})
}

func UpdateProfileTransformed(user_id int64,
		client clients.Client, zipf *zipfgenerator.ZipfGenerator) {

	client.AppRequest([]state.Operation{state.PUT, state.PUT}, []int64{user_id, user_id})
}
//************************************************************//
//***************** Lamernewz VoteComment ********************//
//************************************************************//
// Based on https://github.com/antirez/lamernews/blob/d08bf6baa81216805561f3e5500e43a9dc32c7df/app.rb#L913
/*
post '/api/votecomment' ("comment_id","vote_type") do
  if !r.hget(news_id, comment_id) {
    return false
  }

  //Comments.edit(news_id,comment_id,{vote_type.to_s => varray})
  old = r.hget(key,comment_id)
  return false if !old
  comment = JSON.parse(old).merge(updates)
  r.hset(key,comment_id,comment.to_json)

end
*/

func VoteCommentTransformed(client clients.Client, zipf *zipfgenerator.ZipfGenerator) {
	threadkey := int64(zipf.Uint64())
	client.AppRequest([]state.Operation{state.GET}, []int64{threadkey})

  client.AppRequest([]state.Operation{state.GET}, []int64{threadkey})
	client.AppRequest([]state.Operation{state.PUT}, []int64{threadkey})
}

func VoteCommentSequential(client clients.Client, zipf *zipfgenerator.ZipfGenerator) {
	threadkey := int64(zipf.Uint64())
	client.AppRequest([]state.Operation{state.GET}, []int64{threadkey})

  client.AppRequest([]state.Operation{state.GET}, []int64{threadkey})
	client.AppRequest([]state.Operation{state.PUT}, []int64{threadkey})
}
//************************************************************//
//******************* Lamernewz GetNews **********************//
//************************************************************//
// https://github.com/antirez/lamernews/blob/d08bf6baa81216805561f3e5500e43a9dc32c7df/app.rb#L939
/*
post '/api/getnews' (start, count) do
  numitems = $r.zcard("news.cron")
  news_ids = $r.zrevrange("news.cron",start,start+(count-1))
  //news = get_news_by_id(news_id)
    news = $r.hgetall("news:#{nid}")
    if !news {
      return []
    }
    $r.hmset("news:#{news["id"]}","rank",real_rank)
    $r.zadd("news.top",real_rank,news["id"])
    $r.hget("user:#{news["user_id"]}","username")

    if $user { //$user is a global variable!
      $r.zscore("news.up:#{n["id"]}",$user["id"])
      $r.zscore("news.down:#{n["id"]}",$user["id"])
    }
  //
end
*/

func GetNewsSequential(news_id int64,
		client clients.Client, zipf *zipfgenerator.ZipfGenerator) {

  client.AppRequest([]state.Operation{state.GET}, []int64{NewsCron})
  client.AppRequest([]state.Operation{state.GET}, []int64{NewsCron})

  get_news_by_idSequential(news_id, user_id, true, client, zipf)
}

func GetNewsTransformed(news_id int64,
		client clients.Client, zipf *zipfgenerator.ZipfGenerator) {

  client.AppRequest([]state.Operation{state.GET, state.GET}, []int64{NewsCron, NewsCron})

  get_news_by_idSequential(news_id, user_id, true, client, zipf, nil, nil, nil, nil)
}

//************************************************************//
//***************** Lamernewz GetComments ********************//
//************************************************************//
// Based on https://github.com/antirez/lamernews/blob/d08bf6baa81216805561f3e5500e43a9dc32c7df/app.rb#L958
/*
get  '/api/getcomments/:news_id' (news_id) do  
  //news = get_news_by_id(news_id)
    news = $r.hgetall("news:#{nid}")
    if !news {
      return []
    }
    $r.hmset("news:#{news["id"]}","rank",real_rank)
    $r.zadd("news.top",real_rank,news["id"])
    $r.hget("user:#{news["user_id"]}","username")

    if $user { //$user is a global variable!
      $r.zscore("news.up:#{n["id"]}",$user["id"])
      $r.zscore("news.down:#{n["id"]}",$user["id"])
    }
  //
  thread = r.hgetall(thread_key(news_id))
  for i in range(replies) {
    $r.hgetall("user:#{id}")
  }
end
*/

func GetCommentsSequential(user_id int64, news_id int64,
		client clients.Client, zipf *zipfgenerator.ZipfGenerator) {

  get_news_by_idSequential(news_id, user_id, true, client, zipf)

	threadkey := int64(zipf.Uint64())
	client.AppRequest([]state.Operation{state.GET}, []int64{threadkey})

  /*
  comments := 10
  for i := 0; i < comments; i++ {
    thisuser_id := int64(zipf.Uint64())
    client.AppRequest([]state.Operation{state.GET}, []int64{thisuser_id})
  }*/

  client.AppRequest([]state.Operation{state.GET}, []int64{user_id})
}

func GetCommentsTransformed(user_id int64, news_id int64,
		client clients.Client, zipf *zipfgenerator.ZipfGenerator) {

  get_news_by_idTransformed(news_id, user_id, true, client, zipf, nil, nil, nil, nil)

	threadkey := int64(zipf.Uint64())
	client.AppRequest([]state.Operation{state.GET}, []int64{threadkey})

  /*
  comments := 10
  for i := 0; i < comments; i++ {
    thisuser_id := int64(zipf.Uint64())
    client.AppRequest([]state.Operation{state.GET}, []int64{thisuser_id})
  }*/

  client.AppRequest([]state.Operation{state.GET}, []int64{user_id})
}

/******************************************************************************/
/******************************************************************************/
/********************************** HELPERS ***********************************/
/******************************************************************************/
/******************************************************************************/


//************************************************************//
//********************** get_news_by_id **********************//
//************************************************************//
// Based on https://github.com/antirez/lamernews/blob/d08bf6baa81216805561f3e5500e43a9dc32c7df/app.rb#L958
/*
def get_news_by_id(news_ids) {  
  news = $r.hgetall("news:#{nid}")
  if !news {
    return []
  }
  if opt {
    $r.hmset("news:#{news["id"]}","rank",real_rank)
    $r.zadd("news.top",real_rank,news["id"])
  }
  $r.hget("user:#{news["user_id"]}","username")
  
  if $user { //$user is a global variable!
    $r.zscore("news.up:#{n["id"]}",$user["id"])
    $r.zscore("news.down:#{n["id"]}",$user["id"])
  }
}
*/
func get_news_by_idSequential(news_id int64, user_id int64, opt bool,
		client clients.Client, zipf *zipfgenerator.ZipfGenerator) []int64 {
  client.AppRequest([]state.Operation{state.GET}, []int64{news_id})
  newsup = int64(zipf.Uint64())
  newsdown = int64(zipf.Uint64())
	if opt {
    client.AppRequest([]state.Operation{state.PUT}, []int64{news_id})
    client.AppRequest([]state.Operation{state.PUT}, []int64{NewsTop})
  }
  client.AppRequest([]state.Operation{state.GET}, []int64{user_id})
	client.AppRequest([]state.Operation{state.GET}, []int64{newsup})
	client.AppRequest([]state.Operation{state.GET}, []int64{newsdown})

  return []int64{newsup, newsdown}
}

func get_news_by_idTransformed(news_id int64, user_id int64, opt bool,
		client clients.Client, zipf *zipfgenerator.ZipfGenerator,
    opsbefore []state.Operation, keysbefore []int64,
    opsafter []state.Operation, keysafter []int64) []int64 {
  /*var opTypes []state.Operation
  var keys []int64
  keys = append(keys, user_id)
	opTypes = append(opTypes, state.GET)*/
  client.AppRequest(append(opsbefore, []state.Operation{state.GET}), append(keysbefore, []int64{news_id}))
  newsup = int64(zipf.Uint64())
  newsdown = int64(zipf.Uint64())
	if opt {
    client.AppRequest(append([]state.Operation{state.PUT, state.PUT, state.GET, state.GET, state.GET}, opsafter),
                        append([]int64{news_id, NewsTop, user_id, newsup, newsdown}, keysafter))
  } else {
    client.AppRequest(append([]state.Operation{state.GET, state.GET, state.GET}, opsafter),
                        append([]int64{user_id, newsup, newsdown}, keysafter))
  }
  return []int64{newsup, newsdown}
}

func catchKill(interrupt chan os.Signal) {
	<-interrupt
	if *cpuProfile != "" {
		pprof.StopCPUProfile()
	}
	log.Printf("Caught signal and stopped CPU profile before exit.\n")
	os.Exit(0)
}
