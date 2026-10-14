const assert = require('node:assert/strict');
const {ethers} = require('hardhat');
describe('UzoAgroTrace', function () {
  it('keeps batch fingerprints immutable and restricts checkpoint writers', async function () {
    const [farmer, stranger, aggregator] = await ethers.getSigners();
    const c = await ethers.deployContract('UzoAgroTrace');
    const id = ethers.sha256(ethers.toUtf8Bytes('UZA-TEST'));
    const hash = ethers.sha256(ethers.toUtf8Bytes('record'));
    await (await c.createBatch(id, 'Cassava', hash)).wait();
    assert.equal((await c.getBatch(id)).dataHash, hash);
    await assert.rejects(c.createBatch(id, 'Maize', hash));
    await assert.rejects(c.connect(stranger).addCheckpoint(id, 'Picked up', 'Ogbaru', hash));
    await assert.rejects(c.connect(stranger).setOperator(id, stranger.address, true));
    await (await c.setOperator(id, aggregator.address, true)).wait();
    await (await c.connect(aggregator).addCheckpoint(id, 'Picked up', 'Ogbaru', hash)).wait();
    const cps = await c.getBatchCheckpoints(id);
    assert.equal(cps.length, 1);
    assert.equal(cps[0].recordedBy, aggregator.address);
    assert.equal((await c.getBatch(id)).creator, farmer.address);
    await (await c.setOperator(id, aggregator.address, false)).wait();
    await assert.rejects(c.connect(aggregator).addCheckpoint(id, 'Received', 'Onitsha', hash));
  });
});
