// Copyright 2015-2016, Google, Inc.
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

const pubsub = require(`@google-cloud/pubsub`)();
const uuid = require(`node-uuid`);
const path = require(`path`);
const run = require(`../../utils`).run;

const cwd = path.join(__dirname, `..`);
const topicName = `nodejs-docs-samples-test-${uuid.v4()}`;
const subscriptionName = `nodejs-docs-samples-test-sub-${uuid.v4()}`;
const projectId = process.env.GCLOUD_PROJECT;
const fullTopicName = `projects/${projectId}/topics/${topicName}`;
const fullSubscriptionName = `projects/${projectId}/subscriptions/${subscriptionName}`;
const cmd = `node subscriptions.js`;

describe(`pubsub:subscriptions`, () => {
  before((done) => {
    pubsub.createTopic(topicName, (err) => {
      assert.ifError(err);
      done();
    });
  });

  after((done) => {
    pubsub.subscription(subscriptionName).delete(() => {
      // Ignore any error
      pubsub.topic(topicName).delete(() => {
        // Ignore any error
        done();
      });
    });
  });

  it(`should create a subscription`, (done) => {
    const output = run(`${cmd} create ${topicName} ${subscriptionName}`, cwd);
    assert.equal(output, `Subscription ${fullSubscriptionName} created.`);
    pubsub.subscription(subscriptionName).exists((err, exists) => {
      assert.ifError(err);
      assert.equal(exists, true);
      done();
    });
  });

  it(`should get metadata for a subscription`, () => {
    const output = run(`${cmd} get ${subscriptionName}`, cwd);
    const expected = `Subscription: ${fullSubscriptionName}` +
      `\nTopic: ${fullTopicName}` +
      `\nPush config: {"pushEndpoint":"","attributes":{}}` +
      `\nAck deadline (s): 10`;
    assert.equal(output, expected);
  });

  it(`should list all subscriptions`, (done) => {
    // Listing is eventually consistent. Give the indexes time to update.
    setTimeout(() => {
      const output = run(`${cmd} list`, cwd);
      assert.notEqual(output.indexOf(`Subscriptions:`), -1);
      assert.notEqual(output.indexOf(fullSubscriptionName), -1);
      done();
    }, 5000);
  });

  it(`should list subscriptions for a topic`, (done) => {
    // Listing is eventually consistent. Give the indexes time to update.
    setTimeout(() => {
      const output = run(`${cmd} list ${topicName}`, cwd);
      assert.notEqual(output.indexOf(`Subscriptions for ${topicName}:`), -1);
      assert.notEqual(output.indexOf(fullSubscriptionName), -1);
      done();
    }, 5000);
  });

  it(`should pull messages`, (done) => {
    const expected = `Hello, world!`;
    pubsub.topic(topicName).publish({ data: expected }, (err, messageIds) => {
      assert.ifError(err);
      setTimeout(() => {
        const output = run(`${cmd} pull ${subscriptionName}`, cwd);
        const expectedOutput = `Received ${messageIds.length} messages.\n` +
          `* ${messageIds[0]} "${expected}" {}`;
        assert.equal(output, expectedOutput);
        done();
      }, 5000);
    });
  });

  it(`should set the IAM policy for a subscription`, (done) => {
    run(`${cmd} setPolicy ${subscriptionName}`, cwd);
    pubsub.subscription(subscriptionName).iam.getPolicy((err, policy) => {
      assert.ifError(err);
      assert.deepEqual(policy.bindings, [
        {
          role: `roles/pubsub.editor`,
          members: [`group:cloud-logs@google.com`]
        },
        {
          role: `roles/pubsub.viewer`,
          members: [`allUsers`]
        }
      ]);
      done();
    });
  });

  it(`should get the IAM policy for a subscription`, (done) => {
    pubsub.subscription(subscriptionName).iam.getPolicy((err, policy) => {
      assert.ifError(err);
      const output = run(`${cmd} getPolicy ${subscriptionName}`, cwd);
      assert.equal(output, `Policy for subscription: ${JSON.stringify(policy)}.`);
      done();
    });
  });

  it(`should test permissions for a subscription`, () => {
    const output = run(`${cmd} testPermissions ${subscriptionName}`, cwd);
    assert.notEqual(output.indexOf(`Tested permissions for subscription`), -1);
  });

  it(`should delete a subscription`, (done) => {
    const output = run(`${cmd} delete ${subscriptionName}`, cwd);
    assert.equal(output, `Subscription ${fullSubscriptionName} deleted.`);
    pubsub.subscription(subscriptionName).exists((err, exists) => {
      assert.ifError(err);
      assert.equal(exists, false);
      done();
    });
  });
});
