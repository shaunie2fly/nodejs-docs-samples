// Copyright 2016, Google, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

'use strict';

var pubsubClient = require('@google-cloud/pubsub')();

// [START pubsub_create_subscription]
function createSubscription (topicName, subscriptionName, callback) {
  // References an existing topic, e.g. "my-topic"
  var topic = pubsubClient.topic(topicName);

  // Creates a new subscription, e.g. "my-new-subscription"
  topic.subscribe(subscriptionName, function (err, subscription) {
    if (err) {
      callback(err);
      return;
    }

    console.log('Created subscription: %s', subscription.name);
    callback();
  });
}
// [END pubsub_create_subscription]

// [START pubsub_delete_subscription]
function deleteSubscription (subscriptionName, callback) {
  // References an existing subscription, e.g. "my-subscription"
  var subscription = pubsubClient.subscription(subscriptionName);

  // Deletes the subscription
  subscription.delete(function (err) {
    if (err) {
      callback(err);
      return;
    }

    console.log('Deleted subscription: %s', subscription.name);
    callback();
  });
}
// [END pubsub_delete_subscription]

function getSubscriptionMetadata (subscriptionName, callback) {
  var pubsub = PubSub();
  var subscription = pubsub.subscription(subscriptionName);

  // Get the metadata for the specified subscription
  // See https://googlecloudplatform.github.io/google-cloud-node/#/docs/pubsub/latest/pubsub/subscription?method=getMetadata
  subscription.getMetadata(function (err, metadata) {
    if (err) {
      return callback(err);
    }

    console.log('Got metadata for subscription: %s', subscriptionName);
    return callback(null, metadata);
  });
}

// [START pubsub_list_subscriptions]
function listSubscriptions (callback) {
  // Lists all subscriptions in the current project
  pubsubClient.getSubscriptions(function (err, subscriptions) {
    if (err) {
      callback(err);
      return;
    }

    console.log('Subscriptions:');
    subscriptions.forEach(function (subscription) {
      console.log(subscription.name);
    });
    callback();
  });
}
// [END pubsub_list_subscriptions]

// [START pubsub_list_topic_subscriptions]
function listTopicSubscriptions (topicName, callback) {
  // Reference an existing topic, e.g. "my-topic"
  // See https://googlecloudplatform.github.io/google-cloud-node/#/docs/google-cloud/latest/pubsub?method=topic
  var topic = pubsubClient.topic(topicName);

  // Lists all subscriptions for the topic
  // See https://googlecloudplatform.github.io/google-cloud-node/#/docs/pubsub/latest/pubsub/topic?method=getSubscriptions
  topic.getSubscriptions(function (err, subscriptions) {
    if (err) {
      callback(err);
      return;
    }

    console.log('Subscriptions:');
    subscriptions.forEach(function (subscription) {
      console.log(subscription.name);
    });
    callback();
  });
}
// [END pubsub_list_topic_subscriptions]

function handleMessage (message) {
  console.log('received message: ' + message.data);
}

function pullMessages (subscriptionName, callback) {
  var pubsub = PubSub();
  var subscription = pubsub.subscription(subscriptionName);

  // Pull any messages on the subscription
  // See https://googlecloudplatform.github.io/google-cloud-node/#/docs/pubsub/latest/pubsub/subscription?method=pull
  subscription.pull(function (err, messages) {
    if (err) {
      return callback(err);
    }
    // Do something for each message
    messages.forEach(handleMessage);

    console.log('Pulled %d message(s)!', messages.length);

    var ackIds = messages.map(function (message) {
      return message.ackId;
    });

    // Acknowledge messages
    // See https://googlecloudplatform.github.io/google-cloud-node/#/docs/pubsub/latest/pubsub/subscription?method=ack
    subscription.ack(ackIds, function (err, apiResponse) {
      if (err) {
        return callback(err);
      }

      console.log('Acked %d message(s)!', messages.length);
      return callback(null, messages, apiResponse);
    });
  });
}

// The command-line program
var cli = require('yargs');
var makeHandler = require('../utils').makeHandler;

var program = module.exports = {
  createSubscription: createSubscription,
  deleteSubscription: deleteSubscription,
  getSubscriptionMetadata: getSubscriptionMetadata,
  pullMessages: pullMessages,
  listSubscriptions: listSubscriptions,
  listTopicSubscriptions: listTopicSubscriptions,
  main: function (args) {
    // Run the command-line program
    cli.help().strict().parse(args).argv;
  }
};

cli
  .demand(1)
  .command('create <topicName> <subscriptionName>', 'Creates a new subscription.', {}, function (options) {
    program.createSubscription(options.topicName, options.subscriptionName, makeHandler(false));
  })
  .command('list [topicName]', 'Lists all subscriptions in the current project, optionally filtering by a topic.', {}, function (options) {
    if (options.topicName) {
      program.listTopicSubscriptions(options.topicName, makeHandler(false));
    } else {
      program.listSubscriptions(makeHandler(false));
    }
  })
  .command('get <subscriptionName>', 'Gets the metadata for a subscription.', {}, function (options) {
    program.getSubscriptionMetadata(options.subscriptionName, makeHandler());
  })
  .command('pull <subscriptionName>', 'Pulls messages for a subscription.', {}, function (options) {
    program.pullMessages(options.subscriptionName, makeHandler(false));
  })
  .command('delete <subscriptionName>', 'Deletes a subscription.', {}, function (options) {
    program.deleteSubscription(options.subscriptionName, makeHandler(false));
  })
  .example('node $0 create greetings greetings-worker-1', 'Creates a subscription named "greetings-worker-1" to a topic named "greetings".')
  .example('node $0 delete greetings-worker-1', 'Deletes a subscription named "greetings-worker-1".')
  .example('node $0 pull greetings-worker-1', 'Pulls messages for a subscription named "greetings-worker-1".')
  .example('node $0 list', 'Lists all subscriptions in the current project.')
  .example('node $0 list greetings', 'Lists all subscriptions for a topic named "greetings".')
  .wrap(120)
  .recommendCommands()
  .epilogue('For more information, see https://cloud.google.com/pubsub/docs');

if (module === require.main) {
  program.main(process.argv.slice(2));
}
