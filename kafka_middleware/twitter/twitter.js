$(document).ready(function () {

    setup = function () {
        console.log("executing ajaxSetup()");
        $.ajaxSetup({
            dataType: 'json',
            accept: 'application/json',
            credentials: 'same-origin',
            xhrFields: {
                withCredentials: true
            },
            crossDomain: true
        });
    };

    //Register/login to twitter ------> hard coded
    register = function () {
        console.log("send the cookie");
        $.post("http://localhost:4567/api/users/gianni", 'json').then(res => {
            
            id = res.message.split("=")[0];
            value = res.message.split("=")[1];
            console.log("the cookie: " + document.cookie);
        });
        setup();
        $.cookie("id", "gianni", {path:"/"} );
    }



    //Triggered by Get Tweets
    loadSearchedTweets = function () {

        tags = [];
        locations = [];
        followedUsers = [];

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
            console.log("tweets: " + JSON.parse(res));

            //Append new data to the modal
            $("#getTweetsBody").append(JSON.parse(res));

            //Show the modal
            $("#getTweets").modal("show");

        });
    }

    streamTweets = function () {

        //Api for tweet streaming
    }

    subscribe = function () {

        tags = [];
        locations = [];
        followedUsers = [];

        var subscription = {
            "tags": tags,
            "followedUsers": followedUsers,
            "locations": locations
        }

        if ($("input[name='Locations']:checked").val()) {
            subscription.locations = $("locationsSearch").val.split(" ");
        }
        if ($("input[name='Tags']:checked").val()) {
            subscription.tags = $("tagsSearch").val.split(" ");
        }
        if ($("input[name='Mentions']:checked").val()) {
            subscription.followedUsers = $("mentionsSearch").val.split(" ");
        }

        $.get("http://localhost:4567/api/subscription", JSON.stringify(subscription), 'json').then(res => {
            console.log("Subscription created " + JSON.parse(res));
        });
    }

    postTweet = function () {
        var tweetText = $("tweetText").val();
        //Get the author from the cookie
        var author = $.cookie('id');
        //Set timestamp (optional)
        var timestamp = Date.now();

        //TODO Add in html field location
        locationPost = ";"

        //Find tags in the tweet
        tagsPost = tweetText.split("#").array.forEach(element => {
            return element.split(" ")[0];
        });

        //find mentions in the tweet
        mentionsPost = tweetText.split("@").array.forEach(element => {
            return element.split(" ")[0];
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
    };

    $(function () {
        register();
    });

});
