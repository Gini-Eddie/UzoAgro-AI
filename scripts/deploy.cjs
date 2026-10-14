const {ethers} = require('hardhat');
async function main() {
  const contract = await ethers.deployContract('UzoAgroTrace');
  await contract.waitForDeployment();
  console.log('TRACE_CONTRACT_ADDRESS=' + await contract.getAddress());
  console.log('Deployment transaction: ' + contract.deploymentTransaction().hash);
}
main().catch(error => { console.error(error.message); process.exitCode = 1; });
