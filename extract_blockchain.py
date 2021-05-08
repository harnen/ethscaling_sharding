import web3
from web3 import Web3
#required middleware for poa (rinkeby) chain
from web3.middleware import geth_poa_middleware
import csv
import sys


#ipc_path = '/home/harnen/.ethereum/geth.ipc'
ipc_path = '../../parity-ethereum/json.ipc'
#ipc_path = '/home/krol/blockchain/geth-linux-amd64-1.9.11-6a62fe39/data/geth.ipc'
#ipc_path = '/space/michal/geth-linux-amd64-1.9.11-6a62fe39/data/geth.ipc'
#ipc_path = '/home/harnen/work/city/hackfs/FilecoinPricingMechanism/data/geth.ipc'
wrong_tran = 0
all_tran = 0
user_tran = 0
contract_tran = 0
contract_create_tran = 0
data = []


if(len(sys.argv) > 1):
    ipc_path = sys.argv[1]


print("Connecting to IPC:", ipc_path)
my_provider = Web3.IPCProvider(ipc_path)
w3 = Web3(my_provider)
w3.middleware_onion.inject(geth_poa_middleware, layer=0)

print("Syncing:", w3.eth.syncing)

if(len(sys.argv) > 2):
    latest_block = int(sys.argv[2])
else:
    latest_block = w3.eth.getBlock('latest')['number']

print("latest block:", latest_block)


#block = w3.eth.getBlock(46147)
#tran = w3.eth.getTransactionByBlock(46147, 0)
#print(w3.parity.traceReplayTransaction('0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060'))


with open('data_' + 'hackfs-' + '.csv', 'w') as output_file:
    fieldnames = ['blockNumber', 'from', 'to', 'contractAddress', 'affected']
    dict_writer = csv.DictWriter(output_file, fieldnames=fieldnames)
    dict_writer.writeheader()
    #46213 is the first block with transactions
    for block_num in range(46213, latest_block):
        block = w3.eth.getBlock(block_num)
        tran_count = w3.eth.getBlockTransactionCount(block_num)
        #print("Block", block_num, block)
        miner = block['author']
        if(tran_count > 0):
            #print(tran_count, "transactions in block", block_num)
            for tran_num in range(0, tran_count):
                tran = w3.eth.getTransactionByBlock(block_num, tran_num)
                if(tran['input'] == '0x'):
                    print(tran)
                    print("input 0x")
                    #quit()
                    
                #print(tran)
                continue
                receipt = w3.eth.getTransactionReceipt(tran['hash'])
                record = {}
                record['blockNumber'] = tran['blockNumber']
                record['from'] = tran['from']
                record['to'] = tran['to']
                record['contractAddress'] = receipt['contractAddress']
                #print("from:", record['from'], 'to:', record['to'], 'contract:', record['contractAddress'])
                #print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                #print(w3.parity.traceReplayTransaction(tran['hash'].hex(), ['vmTrace']))
                #print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                #print(w3.parity.traceReplayTransaction(tran['hash'].hex(), ['trace']))
                #print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                stateDiff = w3.parity.traceReplayTransaction(tran['hash'].hex(), ['stateDiff'])
                #stateDiff['output'] =''
                #print(stateDiff['stateDiff'])
                #print("============================================")
                #print("affected:", stateDiff['stateDiff'].keys())
                affected = []
                for key in stateDiff['stateDiff']:
                    #don't append special addresses or miners of the block
                    if(key == '0x0000000000000000000000000000000000000000' or key == miner):
                        continue
                    affected.append(key)
                #print("affected:", affected)
                record['affected'] = affected
                #print("############################################")
                #print(record)
                
                all_tran += 1
                
                dict_writer.writerow(record)
                #if(len(affected) > 2):
                #    print("Len affected", len(affected))
                #    quit()
                #print(record)

