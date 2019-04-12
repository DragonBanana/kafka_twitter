$(document).ready(function () {
    let LoggedIn = false;

    setup = function () {
        console.log("executing ajaxSetup()");
        $.ajaxSetup({
            dataType: 'json',
            accept: 'application/json',
            /*credentials: 'same-origin',
            xhrFields: {
                withCredentials: true
            },*/
            //crossDomain: true
        });
    };

    //Register/login to twitter ------> hard coded
    register = function () {
        username = $("#usernameField").val();
        console.log("send the cookie");
        $.post("http://localhost:4567/api/users/" + username, 'json').then(res => {

            id = res.message.split("=")[0];
            value = res.message.split("=")[1];
            console.log("the cookie: " + document.cookie);
        });
        setup();
        $.cookie("id", username, {
            path: "/"
        });
        LoggedIn = true;
        if (LoggedIn)
            $("#login").text("Ciao " + username);
    }



    //Triggered by Get Tweets
    loadSearchedTweets = function () {
        if (!LoggedIn) {
            window.alert("Login first!")
        } else {

            tags = [];
            locations = [];
            followedUsers = [];

            //If no checkbox is checked do nothing
            if (!$("input[name='Locations']").is(":checked") &&
                !$("input[name='Mentions']").is(":checked") &&
                !$("input[name='Tags']").is(":checked")) {
                return;
            }


            //If the box is checked split the filters deleting the spaces and
            //join them into a unique string ( "&" is the separator), otherwise use "all"
            if ($("input[name='Locations']:checked").val()) {
                locations = $("#locationsSearch").val().split(" ").join("&");
            } else {
                locations = "all";
            }
            if ($("input[name='Tags']:checked").val()) {
                tags = $("#tagsSearch").val().split(" ").join("&");
            } else {
                tags = "all";
            }
            if ($("input[name='Mentions']:checked").val()) {
                followedUsers = $("#mentionsSearch").val().split(" ").join("&");
            } else {
                followedUsers = "all";
            }

            console.log(locations);
            console.log(tags);
            console.log(followedUsers);

            //Create the path for the API
            path = locations + "/" + tags + "/" + followedUsers + "/latest";

            //REST API
            $.get("http://localhost:4567/api/tweets/" + path).then(res => {
                //console.log("tweets: " + JSON.parse(res));

                //Append new data to the modal
                $.each(res, function (index, element) {
                    $("#getTweetsBody").prepend(createTweet(element.Author, element.timestamp, element.content, element.location, element.tags, element.mentions));
                });

                //Show the modal
                $("#getTweets").modal("show");

                //Append new data to the modal
                console.log(res);
                $.each(res, function (index, element) {
                    $("#timeline").prepend(createTweet(element.Author, element.timestamp, element.content, element.location, element.tags, element.mentions));
                });
            });
        }
    }

    closeModal = function () {

        //Close the modal
        $("#getTweets").modal("show");

        //Clear modal body
        $("#getTweetsBody").empty();
    }

    //Triggered by Get Tweets
    subscribeTweets = function () {
        if (!LoggedIn) {
            window.alert("Login first!")
        } else {

            tags = [];
            locations = [];
            followedUsers = [];

            if (!$("input[name='LocationsS']").is(":checked") &&
                !$("input[name='MentionsS']").is(":checked") &&
                !$("input[name='TagsS']").is(":checked")) {
                return;
            }

            //If the box is checked split the filters deleting the spaces and
            //join them into a unique string ( "&" is the separator), otherwise use "all"
            if ($("input[name='LocationsS']:checked").val()) {
                locations = $("#locationsSubscribe").val().split(" ").join("&");
            }
            if ($("input[name='TagsS']:checked").val()) {
                tags = $("#tagsSubscribe").val().split(" ").join("&");
            }
            if ($("input[name='MentionsS']:checked").val()) {
                followedUsers = $("#mentionsSubscribe").val().split(" ").join("&");
            }

            console.log(locations);
            console.log(tags);
            console.log(followedUsers);

            //Create the path for the API
            path = locations + "/" + tags + "/" + followedUsers;

            //REST API
            $.post("http://localhost:4567/api/tweets/subscription/" + path).then(res => {
                console.log(res);
            });
        }
    }


    streamTweets = function () {
        if (!LoggedIn) {
            window.alert("Login first!")
        } else {
            const url = 'ws://localhost:4567/ws';
            const webSocket = new WebSocket(url);
            webSocket.onmessage = function (event) {
                var tweet = JSON.parse(event.data);
                $("#timeline").prepend(createTweet(tweet.Author, tweet.timestamp, tweet.content, tweet.location, tweet.tags, tweet.mentions));
            }
            //Api for tweet streaming
        }
    }

    subscribe = function () {

        if (!LoggedIn) {
            window.alert("Login first!")
        } else {
            tags = [];
            locations = [];
            followedUsers = [];

            var subscription = {
                "tags": tags,
                "followedUsers": followedUsers,
                "locations": locations
            }

            if ($("input[name='LocationsS']:checked").val()) {
                subscription.locations = $("locationsSearch").val.split(" ");
            }
            if ($("input[name='TagsS']:checked").val()) {
                subscription.tags = $("tagsSearch").val.split(" ");
            }
            if ($("input[name='MentionsS']:checked").val()) {
                subscription.followedUsers = $("mentionsSearch").val.split(" ");
            }

            $.get("http://localhost:4567/api/subscription", JSON.stringify(subscription), 'json').then(res => {
                console.log("Subscription created " + JSON.parse(res));
            });
        }
    }

    postTweet = function () {
        if (!LoggedIn) {
            window.alert("Login first!");
        } else {
            var tweetText = $("#tweetText").val();
            //Get the author from the cookie
            var author = $.cookie('id');
            //Set timestamp (optional)
            var timestamp = Date.now();

            var tagsPost = [];

            var mentionsPost = [];

            //TODO Add in html field location
            if ($("#locationPost").val()) {
                locationPost = $("#locationPost").val();
            } else {
                alert("Insert the location");
                return;
            }

            //Split string into an array containing all the words
            tweetArray = tweetText.split(" ");
            $.each(tweetArray, function (index, element) {
                if (element.startsWith("@")) {
                    mentionsPost.push(element);
                }
                if (element.startsWith("#")) {
                    tagsPost.push(element);
                }

            });

            var tweet = {
                "author": author,
                "content": tweetText,
                "timestamp": timestamp,
                "location": locationPost,
                "tags": tagsPost,
                "mentions": mentionsPost
            }
            console.log(JSON.stringify(tweet));

            //REST Api to post the tweet
            $.post("http://localhost:4567/api/tweets", JSON.stringify(tweet), 'json').then(res => {
                if (res == 200) {
                    //Append the new tweet to the timeline
                    console.log("Ok post");
                } else {
                    console.log("Something went wrong post")
                }
            });
        }
    };


    function createTweet(author, timestamp, content, location, tags, mentions) {
        if (!LoggedIn) {
            window.alert("Login first!")
        } else {
            return '<li> \
                    <div class="timeline-badge"><i class="glyphicon glyphicon-check"></i></div> \
                    <div class="timeline-panel"> \
                      <div class="timeline-heading"> \
                        <h4 class="timeline-title">' + author + '</h4> \
                        <p><small class="text-muted"><i class="glyphicon glyphicon-time"></i>' + timestamp + ' at ' + location + '</small> \
                        </p> \
                      </div> \
                      <div class="timeline-body"> \
                        <p>' + content + '</p> \
                      </div> \
                    </div> \
                  </li>'
        }
    };


});