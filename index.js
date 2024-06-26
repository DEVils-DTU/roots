import "dotenv/config";
import stream from "./helpers/streamBlock.js";
import { createQueue } from "./helpers/rapidQueue.js";
import { MongoClient } from "mongodb";

const queue = createQueue();
// The launch block
const genesisBlock = 86605488;
let firstRun = true;
let totalSyncedBlocks = 0;
const uri = `mongodb+srv://${process.env.dbuser}:${process.env.dbpass}@${process.env.dburl}/`;
const client = new MongoClient(uri);

async function start() {
  try {
    // await initDatabase();
    stream.streamBlockNumber(async (blockNum) => {
      if (!blockNum) {
        return;
      }
      if (firstRun) {
        firstRun = false;
        await queueOldBlocks(blockNum);
      }
      queue.push(blockNum);
    });
    processQueue();
  } catch (e) {
    throw new Error(e);
  }
}

async function queueOldBlocks(nowBlock) {
  let oldestBlock = await getLastBlock();
  if (oldestBlock < nowBlock) {
    for (let i = oldestBlock; i < nowBlock; i++) {
      queue.push(i);
    }
  }
}

const intervalTime = 5; // 50ms
const maxI = 1;
let queueIndex = 0;
function processQueue() {
  setInterval(() => {
    const L = queue.length();
    if (queueIndex < maxI && L > 0) {
      const n = maxI - queueIndex > L ? L : maxI - queueIndex;
      for (let k = 0; k < n; k++) {
        const blockNum = queue.shift();
        processBlock(blockNum);
      }
    }
  }, intervalTime);
}

async function processBlock(blockNum) {
  if (!blockNum) {
    return;
  }
  queueIndex++;
  try {
    const operations = await stream.getOperations(blockNum);
    if (operations && operations.length > 0) {
      for (const ops of operations) {
        for (const op of ops) {
          //   console.log(op);
          if (op && op[0] === "custom_json" && op[1].id === "sproutapp") {
            // console.log(JSON.stringify(op, null, 2));
            await processData(op[1].json, [
              ...op[1].required_posting_auths,
              ...op[1].required_auths,
            ]);
          }
        }
      }
    }
    await updateLastblock(blockNum);
    totalSyncedBlocks++;
  } catch (e) {}
  queueIndex--;
}

async function getLastBlock() {
  const database = client.db("sprout");
  const blockTracking = database.collection("blockTracking");
  const lastBlock = await blockTracking.findOne({});

  //   console.log(lastBlock);
  if (lastBlock && lastBlock.blockNumber) {
    return lastBlock.blockNumber;
  }
}

async function updateLastblock(blockNum) {
  if (!blockNum) {
    return;
  }
  const database = client.db("sprout");
  const blockTracking = database.collection("blockTracking");
  const lastBlock = await blockTracking.findOne({});
  if (lastBlock) {
    let a = await blockTracking.updateOne(
      { _id: lastBlock._id },
      { $set: { blockNumber: blockNum } }
    );
    // console.log(a);
  }
}

async function processData(jsonData, postingAuths) {
  try {
    console.log(jsonData, postingAuths);
    if (!jsonData) {
      return;
    }
    const data = JSON.parse(jsonData);
    if (!data || !data.action || !data.app) {
      return;
    }
    if (
      !postingAuths ||
      !Array.isArray(postingAuths) ||
      postingAuths.length < 1
    ) {
      return;
    }
    const user = postingAuths[0];
    console.log(data, user);
    if (data.action === "create_posting") {
      await createPosting(data, user);
    } else if (data.action === "delete_posting") {
      console.log("delete");
      await deletePosting(data, user);
    } else if (data.action === "create_offer") {
      await createOffer(data, user);
    } else if (data.action === "reply_to_offer") {
      await replyToOffer(data, user);
    }
    // Create Posting
    // Delete Posting
    // Create Offer
    // Reply to Offer
  } catch (e) {
    // error might be on JSON.parse and wrong json format
    console.log(e);
    return null;
  }
}

async function createPosting(data, user) {
  // a={
  //     app: "sprout/0.0.1",
  //     action: "create_posting",
  //     postingID: "",
  //     authorDisplay: "DisplayName",
  //     title: "title",
  //     coverImageURL: "coverImageURL",
  //     text: "text",
  //     preview: "preview",
  //     posterContactInfo: "posterContactInfo",
  //     proposedPrice: 0,
  //   }
  if (data.postingID.length != 36) {
    return;
  }
  const database = client.db("sprout");
  const postings = database.collection("postings");

  // check for postings with same postingID
  const postingExists = await postings.findOne({ postingID: data.postingID });
  if (postingExists) {
    return;
  }

  const result = await postings.insertOne({
    postingID: data.postingID,
    author: user,
    authorDisplay: data.authorDisplay,
    title: data.title,
    coverImageURL: data.coverImageURL,
    text: data.text,
    preview: data.preview,
    posterContactInfo: data.posterContactInfo,
    proposedPrice: data.proposedPrice,
    status: "open",
  });
  console.log(result);
}

async function deletePosting(data, user) {
  // a={
  //     app: "sprout/0.0.1",
  //     action: "delete_posting",
  //     postingID: "",
  //   }
  if (data.postingID.length != 36) {
    return;
  }
  const database = client.db("sprout");
  const postings = database.collection("postings");

  const posting = await postings.findOne({ postingID: data.postingID });
  console.log(posting, user);
  if (!posting) {
    return;
  }
  if (posting.author !== user) {
    return;
  }
  // update posting status to closed
  const result = await postings.updateOne(
    { postingID: data.postingID },
    { $set: { status: "closed" } }
  );
  console.log(result);
}

async function createOffer(data, user) {
  // a={
  //     app: "sprout/0.0.1",
  //     action: "create_offer",
  //     postingID: "",
  //     offerID: "",
  //     offererDisplay: "",
  //     amount: 0,
  //     offererContactInfo: "",
  //   }
  if (data.postingID.length != 36) {
    return;
  }
  const database = client.db("sprout");
  const offers = database.collection("offers");
  const postings = database.collection("postings");

  // check for offers with same offerID
  const offerExists = await offers.findOne({ offerID: data.offerID });
  if (offerExists) {
    return;
  }
  const postingExists = await postings.findOne({ postingID: data.postingID });
  if (!postingExists) {
    return;
  }

  const result = await offers.insertOne({
    postingID: data.postingID,
    offerID: data.offerID,
    offerer: user,
    offererDisplay: data.offererDisplay,
    amount: data.amount,
    offererContactInfo: data.offererContactInfo,
    status: "open",
  });
  console.log(result);
}

async function replyToOffer(data, user) {
  // a={
  //     app: "sprout/0.0.1",
  //     action: "reply_to_offer",
  //     offerID: "",
  //     status: "accepted/rejected"
  //   }
  if (data.offerID.length != 36) {
    return;
  }
  const database = client.db("sprout");
  const offers = database.collection("offers");
  const postings = database.collection("postings");

  const offer = await offers.findOne({ offerID: data.offerID });
  if (!offer) {
    return;
  }
  const postingID = offer.postingID;
  const posting = await postings.findOne({ postingID: postingID });
  if (!posting) {
    return;
  }
  if (posting.author !== user) {
    return;
  }
  // update offer status
  const result = await offers.updateOne(
    { offerID: data.offerID },
    { $set: { status: data.status } }
  );
}
start();
console.log("Starting application...");
const interval = setInterval(() => {
  if (queue.length() < 2) {
    clearInterval(interval);
    console.log("Sync completed. Application is running.");
  } else {
    console.log("Syncing blocks... Total synced blocks: " + totalSyncedBlocks);
  }
}, 5000);
