import { call } from "hive-tx";

const INTERVAL_TIME = 1000;

const streamBlockNumber = async (cb) => {
  try {
    let lastBlock = 0;
    setInterval(async () => {
      const gdgp = await call("condenser_api.get_dynamic_global_properties");
      if (
        gdgp &&
        gdgp.result &&
        gdgp.result.head_block_number &&
        !isNaN(gdgp.result.head_block_number)
      ) {
        if (gdgp.result.head_block_number > lastBlock) {
          lastBlock = gdgp.result.head_block_number;
          console.log("Last block: ", lastBlock);
          cb(lastBlock);
        }
      }
    }, INTERVAL_TIME);
  } catch (e) {
    throw new Error(e);
  }
};

const getOperations = async (blockNumber) => {
  const result = await call("condenser_api.get_block", [blockNumber]);
  if (result.result) {
    const operations = result.result.transactions.map((transaction) => {
      return transaction.operations;
    });
    return operations;
  }
};

export default {
  streamBlockNumber,
  getOperations,
};
