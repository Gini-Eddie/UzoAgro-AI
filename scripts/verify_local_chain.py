"""Integration check against a disposable Hardhat RPC (no testnet transactions)."""
import json
import os
import sys
import tempfile
from datetime import date
from pathlib import Path

from web3 import Web3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ['DATABASE_URL'] = ''
os.environ['BASE_SEPOLIA_RPC_URL'] = 'http://127.0.0.1:8545'

import trace_api as trace

def main():
    artifact = json.loads(Path('artifacts/contracts/UzoAgroTrace.sol/UzoAgroTrace.json').read_text())
    web3 = Web3(Web3.HTTPProvider(os.environ['BASE_SEPOLIA_RPC_URL']))
    assert web3.eth.chain_id == 84532
    farmer = web3.eth.accounts[0]
    contract = web3.eth.contract(abi=artifact['abi'], bytecode=artifact['bytecode'])
    receipt = web3.eth.wait_for_transaction_receipt(contract.constructor().transact({'from': farmer}))
    os.environ['TRACE_CONTRACT_ADDRESS'] = receipt.contractAddress
    contract = web3.eth.contract(address=receipt.contractAddress, abi=artifact['abi'])
    with tempfile.TemporaryDirectory() as tmp:
        os.environ['TRACE_SQLITE_PATH'] = str(Path(tmp) / 'test.db')
        owner = {'name': 'Integration Farmer', 'phone': '00000000000', 'community': 'Ogbaru'}
        b = trace.create_batch(trace.NewBatch(crop='Cassava', quantity=250, community='Ogbaru', harvest_date=date.today()), owner)
        key = bytes.fromhex(b['chain_key'])
        tx = contract.functions.createBatch(key, 'Cassava', bytes.fromhex(b['hash'])).transact({'from': farmer})
        web3.eth.wait_for_transaction_receipt(tx)
        trace.anchor(b['id'], trace.Anchor(transaction=Web3.to_hex(tx)), owner)
        assert trace.verify(b['id'])['blockchain_verified']
        for stage, location in [('Picked up', 'Ogbaru'), ('Received at hub', 'Onitsha'), ('Delivered to buyer', 'Market')]:
            b = trace.add_checkpoint(b['id'], trace.NewCheckpoint(stage=stage, location=location), owner)
            cp = b['checkpoints'][-1]
            tx = contract.functions.addCheckpoint(key, stage, location, bytes.fromhex(cp['hash'])).transact({'from': farmer})
            web3.eth.wait_for_transaction_receipt(tx)
            trace.anchor(b['id'], trace.Anchor(transaction=Web3.to_hex(tx), checkpoint_id=cp['record']['id']), owner)
        result = trace.verify(b['id'])
        assert result['blockchain_verified'] and result['checkpoints_verified'] and result['integrity']
        stored = trace.get('batch:' + b['id'])
        stored['metadata']['quantity'] += 1
        trace.put('batch:' + b['id'], stored)
        assert not trace.verify(b['id'])['integrity']
    print('PASS: contract deployment -> batch anchor -> 3 checkpoint anchors -> public verification -> tamper detection')

if __name__ == '__main__':
    main()
