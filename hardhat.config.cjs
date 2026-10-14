require('dotenv').config();
require('@nomicfoundation/hardhat-ethers');
module.exports = {
  solidity: '0.8.24',
  networks: {hardhat: {chainId: 84532}, baseSepolia: {
    url: process.env.BASE_SEPOLIA_RPC_URL || 'https://sepolia.base.org',
    chainId: 84532,
    accounts: process.env.DEPLOYER_PRIVATE_KEY ? [process.env.DEPLOYER_PRIVATE_KEY] : []
  }}
};
