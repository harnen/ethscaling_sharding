import abc
import random
import networkx as nx
from collections import Counter
import csv
#from misc import *
import metis
from policy import Policy
#import nxmetis
import pickle

class PMetisPolicy(Policy):
    def __init__(self, shards_num, fading_rate=0.5, migrate_contracts=True, force_migration = False, debug=False):
        self.num_txs = 0
        self.fading_rate = fading_rate
        self.period = 0
        self.shards_num = shards_num
        self.partition_weights = [1.0/self.shards_num] * self.shards_num
        
        self.inactive_nodes = {}
        self.is_contracts_migrated = migrate_contracts
        self.force_migration = force_migration
        self.G = nx.Graph()
        self.G.graph['node_weight_attr'] = 'num_txs'
        self.G.graph['edge_weight_attr'] = 'weight'
        # virtual nodes (one per partition)
        self.centroid_nodes = [str(i) for i in range(self.shards_num)]
        # mapping of partitions between subsequent execution of metis 
        self.partition_mapping = {}
        self.debug = debug
        # number of migrations
        self.migrations = 0
        # num. of migrations that can not be combined with a intershard tx
        self.expensive_migrations = 0
        # num. of expensive migrations that can be avoided through careful ordering
        self.avoidable_expensive_migrations = 0
        # first period that a contract emerged
        self.contract_start_period = {}
        # contracts that do not move once assigned to a shard (when is_contracts_migrated is False)
        self.static_contracts = set()

        self.addr_classification = pickle.load(open("/home/onur/blockchain_sharding/code/input/classification_500k_lines.pickle", "rb"))

    def add_edge(self, g, node1, node2, weight):
        if isinstance(node1, int):
            raise ValueError('node: ', node1, ' should be string')
        if isinstance(node2, int):
            raise ValueError('node: ', node2, 'should be string')
        if node1 not in g.nodes() or node2 not in g.nodes():
            print ('Error: one of the nodes in add_edge is not in nodes')

        g.add_edge(node1, node2, weight=weight)

    def updatePolicy(self, row):
        
        #num_state_transition_txs = 0
        #num_regular_txs = 0
        #num_new_node_pairs = 0

        from_n = row['from']
        if(row['to'] != ''):
            to_n = row['to']
        else:
            to_n = row['contractAddress']
        

        #if isinstance(from_n, int) or isinstance(to_n, int):
        #    #state transition tx
        #    num_state_transition_txs += 1
        #    continue
        
        #num_regular_txs += 1
        #print('Regular tx: ', from_n, ' ', to_n)

        
        if self.is_contracts_migrated is False:
            if self.addr_classification[from_n] == 'contract' and from_n not in self.contract_start_period:
                self.contract_start_period[from_n] = self.period
            if self.addr_classification[to_n] == 'contract' and to_n not in self.contract_start_period:
                self.contract_start_period[to_n] = self.period
            
        if self.period == 0:
            if from_n not in self.G.nodes():
                self.G.add_node(from_n)
                self.G.nodes[from_n]['shard'] = None
                self.G.nodes[from_n]['num_txs'] = 1
            else:
                self.G.nodes[from_n]['num_txs'] += 1
        
            if to_n not in self.G.nodes():
                self.G.add_node(to_n)
                self.G.nodes[to_n]['shard'] = None
                self.G.nodes[to_n]['num_txs'] = 1
            else:
                self.G.nodes[to_n]['num_txs'] += 1

            if(self.G.has_edge(from_n, to_n)):
                self.G[from_n][to_n]['weight'] += 1
            else:
                self.add_edge(self.G, from_n, to_n, 1)
        else: # period > 0
            # first reactivate the in-active nodes
            if from_n in self.inactive_nodes:
                self.G.add_node(from_n)
                shard = self.inactive_nodes[from_n]['shard']
                weight = self.inactive_nodes[from_n]['weight']
                self.add_edge(self.G, from_n, self.centroid_nodes[shard], weight)
                if weight < 1:
                    self.G[from_n][self.centroid_nodes[shard]]['weight'] = 1
                self.G.nodes[from_n]['num_txs'] = 1
                self.G.nodes[from_n]['shard'] = shard
                self.G.nodes[from_n]['prev_shard'] = shard
                del self.inactive_nodes[from_n]
            if to_n in self.inactive_nodes:
                self.G.add_node(to_n)
                shard = self.inactive_nodes[to_n]['shard']
                weight = self.inactive_nodes[to_n]['weight']
                self.add_edge(self.G, to_n, self.centroid_nodes[shard], weight)
                if weight < 1:
                    self.G[to_n][self.centroid_nodes[shard]]['weight'] = 1
                self.G.nodes[to_n]['num_txs'] = 1
                self.G.nodes[to_n]['shard'] = shard
                self.G.nodes[to_n]['prev_shard'] = shard
                del self.inactive_nodes[to_n]
            
            # if both nodes are not part of the graph, add them to
            # the least-loaded shard
            if from_n not in self.G.nodes() and to_n not in self.G.nodes():
                self.G.add_node(from_n)
                self.G.add_node(to_n)
                self.G.nodes[from_n]['shard'] = None
                self.G.nodes[to_n]['shard'] = None
                self.G.nodes[from_n]['num_txs'] = 1
                self.G.nodes[to_n]['num_txs'] = 1
                
                num_txs = [self.G.nodes[node]['num_txs'] for node in self.centroid_nodes]
                min_txs = min(num_txs)
                min_shard = self.centroid_nodes[num_txs.index(min_txs)]
                
                self.add_edge(self.G, from_n, min_shard, 1)
                self.add_edge(self.G, to_n, min_shard, 1)
                #num_new_node_pairs += 1

            elif from_n in self.G.nodes() and to_n in self.G.nodes():
                self.G.nodes[from_n]['num_txs'] += 1
                self.G.nodes[to_n]['num_txs'] += 1
                # XXX why set prev_shard to None? check this. Commenting out for now
                #self.G.nodes[from_n]['prev_shard'] = None
                #self.G.nodes[to_n]['prev_shard'] = None
                if self.G.has_edge(from_n, to_n):
                    self.G[from_n][to_n]['weight'] += 1
                else:
                    self.add_edge(self.G, from_n, to_n, 1)

            else: # either from_n or to_n is already in G
                if from_n not in self.G.nodes():
                    self.G.add_node(from_n)
                    self.add_edge(self.G, from_n, to_n, 1)
                    self.G.nodes[from_n]['shard'] = None
                    self.G.nodes[from_n]['prev_shard'] = None
                    self.G.nodes[from_n]['num_txs'] = 1
                    self.G.nodes[to_n]['num_txs'] += 1
                else: #to_n is not in self.G.nodes
                    self.G.add_node(to_n)
                    self.add_edge(self.G, from_n, to_n, 1)
                    self.G.nodes[to_n]['shard'] = None
                    self.G.nodes[to_n]['prev_shard'] = None
                    self.G.nodes[to_n]['num_txs'] = 1
                    self.G.nodes[from_n]['num_txs'] += 1

        #print('Reading txs for period: ', self.period)
        #print('Number of regular txs: ', num_regular_txs)
        #print('Number of state transition txs: ', num_state_transition_txs)
        #print('Number of new node pairs: ', num_new_node_pairs)
        
    def prevShard(self, txhash):
        if 'prev_shard' not in self.G.nodes[txhash] or self.G.nodes[txhash]['prev_shard'] is None:
            return self.G.nodes[txhash]['shard']

        if self.G.nodes[txhash]['shard'] != self.G.nodes[txhash]['prev_shard']:
            prev_shard = self.G.nodes[txhash]['prev_shard']
            self.G.nodes[txhash]['prev_shard'] = self.G.nodes[txhash]['shard']
            return prev_shard
        else:
            return self.G.nodes[txhash]['shard']

    def convert_to_star(self):
        H = nx.Graph()
        self.G.graph['part_to_node'] = {}
        for part in range(self.shards_num):
            self.G.graph['part_to_node'][part] = [self.centroid_nodes[part]]

        for node in self.G.nodes():
            if 'centroid' in self.G.nodes[node]:
                continue
            self.G.graph['part_to_node'][self.G.nodes[node]['shard']].append(node)

        # Graph conversion and combining contract nodes with centroids (avoiding the migration of contract nodes)
        for partition in range(self.shards_num):
            if len(self.G.graph['part_to_node'][partition]) > 1:
                nx.add_star(H, self.G.graph['part_to_node'][partition])
            else:
                H.add_node(self.centroid_nodes[partition])
            centroid = self.centroid_nodes[partition]
            H.nodes[centroid]['centroid'] = True
            for node in self.G.graph['part_to_node'][partition]:
                if 'centroid' in H.nodes[node]:
                    H.nodes[node]['shard'] = partition
                    continue
                    
                H.nodes[node]['num_txs'] = self.G.nodes[node]['num_txs']
                H.nodes[node]['shard'] = self.G.nodes[node]['shard']
                if self.G.nodes[node]['shard'] != partition:
                    print('this should not happen')
                
                if self.period > 0:
                    H.nodes[node]['prev_shard'] = self.G.nodes[node]['prev_shard']
                
                total_weight = 0
                H[node][centroid]['weight'] = 1
                for nghbr in self.G.neighbors(node):
                    if self.G.nodes[node]['shard'] == self.G.nodes[nghbr]['shard']:
                        total_weight += self.G[node][nghbr]['weight']

                H[node][centroid]['weight'] = int(total_weight)
                if total_weight == 0:
                    # this happens in some cases
                    H[node][centroid]['weight'] = 1
            H.nodes[centroid]['num_txs'] = 0
            if self.debug:
                print('Centroid node: ', centroid, ' num_txs set to: ', H.nodes[centroid]['num_txs'])

        self.G = H
        self.G.graph['node_weight_attr'] = 'num_txs'
        self.G.graph['edge_weight_attr'] = 'weight'

    def remove_contract_nodes(self):
        nodes_to_remove = []
        for node in self.G.nodes():
            if 'centroid' in self.G.nodes[node]:
                continue
            if self.addr_classification[node] == 'contract' and self.period > self.contract_start_period[node]:
                self.static_contracts.add(node)
                nodes_to_remove.append(node)
                shard = self.G.nodes[node]['shard']
                node_weight = self.G.nodes[node]['num_txs']
                edge_weight = self.G[node][self.centroid_nodes[shard]]['weight']
                self.G.nodes[self.centroid_nodes[shard]]['num_txs'] += node_weight
                weight = self.G[node][self.centroid_nodes[shard]]['weight']
                self.inactive_nodes[node] = {'shard': shard, 'weight': edge_weight}

        for node in nodes_to_remove:
            self.G.remove_node(node)

    def compute_partitions_kl(self):
        """ This method seems too slow"""
        """We can possibly use this to create recursive bisections until
           reaching the desired number of partitions"""
        p1, p2 = nx.algorithms.community.kernighan_lin_bisection(self.G, None, max_iter=2, weight='weight', seed=0)
        print(p1)
        print(p2)

    def compute_partitions_metis(self):
        #colors = ['red','blue','green', 'aliceblue', 'brown', 'cyan', 'darkorchid', 'darkorange', 'darkgoldenrod1', 'gray']
        
        if self.period > 0:
            for centroid in self.centroid_nodes:
                self.G.nodes[centroid]['num_txs'] += 1000

        num_zero_tx_nodes = 0
        for node, degree in self.G.degree():
            if degree == 0 and self.debug:
                print('Node: ', node, ' has degree: ', degree)
            if self.G.nodes[node]['num_txs'] == 0:
                num_zero_tx_nodes += 1
        if self.debug:
            print('Number of nodes with 0 num_txs: ', num_zero_tx_nodes)

                    
        self.G.graph['partition_nodeSum'] = [0]*self.shards_num
        #edge_cuts, self.G.graph['partition'] = metis.part_graph(self.G, self.shards_num, self.partition_weights, ubvec=[1.9])
        edge_cuts, self.G.graph['partition'] = metis.part_graph(self.G, self.shards_num, self.partition_weights)
        if self.debug:
            print ('objval: ', edge_cuts)
        
        partitions = { x:[] for x in range(self.shards_num)} 
        for index, node in enumerate(self.G.nodes()):
            self.G.nodes[node]['shard'] = self.G.graph['partition'][index]
            if 'prev_shard' not in self.G.nodes[node]:
                self.G.nodes[node]['prev_shard'] = self.G.nodes[node]['shard']
            #if self.period == 0:
            #    self.G.nodes[node]['prev_shard'] = self.G.nodes[node]['shard']
            if self.G.nodes[node]['shard'] > self.shards_num:
                print('Error: node ', node, ' has a shard number of ', self.G.nodes[node]['shard'])
            self.G.graph['partition_nodeSum'][self.G.nodes[node]['shard']] += self.G.nodes[node]['num_txs']
            partitions[self.G.nodes[node]['shard']].append(node)

        if self.debug:
            for index, total in enumerate(self.G.graph['partition_nodeSum']):
                if self.debug:
                   print('Partition ', index, ' has total node weight: ', total)
                if total == 0 and self.debug:
                    print ('parition of shard', index, ' is: ', partitions[index])

        #filename = 'example' + str(self.period)
        #nx.drawing.nx_pydot.write_dot(self.G, filename)

        if self.period > 0:
            # map new partition number to existing
            partition_mapping = {}
            unmapped_new_partitions = list(range(self.shards_num))
            unmapped_old_partitions = list(range(self.shards_num))
            for node in self.G.nodes():
                if 'centroid' in self.G.nodes[node]:
                    digits = [d for d in node if d.isdigit()]
                    old_partition = int(''.join(digits))
                    new_partition = self.G.nodes[node]['shard']
                    if new_partition in partition_mapping:
                        continue
                    else:
                        if self.debug:
                            print('Mapping partitions: ', old_partition, ' to ', new_partition) 
                        unmapped_old_partitions.remove(old_partition)
                        unmapped_new_partitions.remove(new_partition)
                        partition_mapping[new_partition] = old_partition
            if len(unmapped_new_partitions) != len(unmapped_old_partitions):
                print('This should not happen')
            for new_part in unmapped_new_partitions:
                partition_mapping[new_part] = unmapped_old_partitions.pop()
                if self.debug:
                    print('Mapping unmapped partition: ', new_part, ' to ', partition_mapping[new_part])

            # map the shard numbers to reflect the original (centroid node numbers)
            if self.debug:
                print('Partition mapping: ', partition_mapping)
            for node in self.G.nodes():
                self.G.nodes[node]['shard'] = partition_mapping[self.G.nodes[node]['shard']]

            # here compute the number of expensive moves (state migrations) 
            # This happens if a node migrates to a third shard where 
            # connection
            num_expensive_migrations = 0
            num_avoidable_expensive_migrations = 0
            for node in self.G.nodes():
                if 'centroid' in self.G.nodes[node]:
                    continue
                    
                for nghbr in self.G.neighbors(node):
                    if 'centroid' in self.G.nodes[nghbr]:
                        continue
                    if self.G.nodes[nghbr]['shard'] != self.G.nodes[node]['shard']:
                        if (self.G.nodes[nghbr]['prev_shard'] is None or self.G.nodes[node]['prev_shard'] is None):
                            num_expensive_migrations += 1

                        elif self.G.nodes[nghbr]['prev_shard'] != self.G.nodes[node]['shard'] and self.G.nodes[nghbr]['shard'] != self.G.nodes[node]['prev_shard'] and self.G.nodes[nghbr]['prev_shard'] != self.G.nodes[node]['prev_shard']:
                            num_expensive_migrations += 1

                        elif (self.G.nodes[nghbr]['prev_shard'] == self.G.nodes[node]['shard'] or self.G.nodes[nghbr]['shard'] == self.G.nodes[node]['prev_shard'] or self.G.nodes[nghbr]['prev_shard'] == self.G.nodes[node]['prev_shard']):
                            num_avoidable_expensive_migrations += 1

            self.expensive_migrations += num_expensive_migrations/2
            self.avoidable_expensive_migrations += num_avoidable_expensive_migrations/2

            # here compute the number of moves (state migrations) and 
            # update prev_shard values for next iteration
            num_migrations = 0
            for node in self.G.nodes():
                if 'centroid' in self.G.nodes[node]:
                    continue
                if (self.G.nodes[node]['prev_shard'] is not None) and (self.G.nodes[node]['shard'] != self.G.nodes[node]['prev_shard']):
                    num_migrations += 1
                
                self.G.nodes[node]['prev_shard'] = self.G.nodes[node]['shard']
            #print ("Number of migrations at iteration ", self.period, " is ", num_migrations)
            self.migrations += num_migrations

    def compute_partitions_louvain(self):
        
        initial_partition = None
        if self.period > 0:
            for partition in range(self.shards_num):
                centroid = 'partition' + str(partition)
                initial_partition[H.nodes[centroid]] = partition

                self.G.graph['partition'] = community.best_partition(self.G, initial_partition)
        else:
            self.G.graph['partition'] = calculateLouvain(self.G, self.shards_num)
        
        self.G.graph['partition_nodeSum'] = [0]*self.shards_num
        for node in self.G.nodes():
            if self.period > 0:
                self.G.nodes[node]['prev_shard'] = self.G.nodes[node]['shard']
            self.G.nodes[node]['shard'] = self.G.graph['partition'][node]
            if self.G.nodes[node]['shard'] > self.shards_num:
                print('Error: node ', node, ' has a shard number of ', self.G.nodes[node]['shard'])
            self.G.graph['partition_nodeSum'][self.G.nodes[node]['shard']] += self.G.nodes[node]['num_txs']

        
        for index, total in enumerate(self.G.graph['partition_nodeSum']):
            print('Partition ', index, ' has total node weight: ', total)

        if self.period > 0:
            # map new partition number to existing
            partition_mapping = {}
            for node in self.G.nodes():
                if 'centroid' in self.G.nodes[node]:
                    print('Node: ', node)
                    digits = [d for d in node if d.isdigit()]
                    old_partition = int(''.join(digits))
                    new_partition = self.G.nodes[node]['shard']
                    if old_partition != new_partition:
                        print('Mapping partitions: ', old_partition, ' to ', new_partition) 
                    partition_mapping[new_partition] = old_partition

            # map the shard numbers to reflect the original (centroid node numbers)
            print('Partition mapping: ', partition_mapping)
            for node in self.G.nodes():
                self.G.nodes[node]['shard'] = partition_mapping[self.G.nodes[node]['shard']]

    def getShardNum(self, txhash):
        if self.is_contracts_migrated is False and txhash in self.static_contracts:
            return self.inactive_nodes[txhash]['shard']
            
        if txhash not in self.G.nodes():
            if self.debug:
                print('Error node: ', txhash, ' is not in G')
            if txhash in self.inactive_nodes and self.debug:
                print ('Error node: ', txhash, ' is in inactive nodes')

        return self.G.nodes[txhash]['shard']

    def reset_node_weights(self):
        for node in self.G.nodes():
            #if 'centroid' in self.G.nodes[node]:
            #    continue
            self.G.nodes[node]['num_txs'] = 0

    def process_inactive_nodes(self, mempool_txs):
        nodes_to_remove = []
        for node in self.G.nodes():
            if 'centroid' in self.G.nodes[node]:
                continue
            if node in mempool_txs: # this node is in leftovers and should be active soon
                continue 
            if self.G.nodes[node]['num_txs'] == 0:
                shard = self.G.nodes[node]['shard']
                weight = self.G[node][self.centroid_nodes[shard]]['weight']
                if node in self.inactive_nodes:
                    print('Error: this a node should not be part of inactive nodes and the active tx graph')
                self.inactive_nodes[node] = {'shard': shard, 'weight': weight}
                nodes_to_remove.append(node)
        if self.debug:
            print ('Removing ', len(nodes_to_remove), ' in-active nodes at period ', self.period)
        for node in nodes_to_remove:
            self.G.remove_node(node)
        
    
    def fade_edges(self):
        # fade the edge weights of in-active nodes at each period
        for node, val in self.inactive_nodes.items():
            weight = val['weight']
            faded_weight = weight
            if self.fading_rate != 0:
                faded_weight = int(weight//(1/self.fading_rate))
            self.inactive_nodes[node] =  {'shard':val['shard'], 'weight':faded_weight}
    def update_prev_shards(self):
        for node in self.G.nodes():
            self.G.nodes[node]['prev_shard'] = self.G.nodes[node]['shard']

    def read_mempool_txs(self, transaction_buffer):

        mempool_txs = set()

        for row in transaction_buffer:

            from_n = row['from']
            if(row['to'] != ''):
                to_n = row['to']
            else:
                to_n = row['contractAddress']
            mempool_txs.add(from_n)
            mempool_txs.add(to_n)

        return mempool_txs

    def analyzeMempoll(self, transaction_buffer):
        if self.period > 0 and self.is_contracts_migrated is False:
            self.remove_contract_nodes()
        mempool_txs = self.read_mempool_txs(transaction_buffer)
        self.process_inactive_nodes(mempool_txs)
        self.compute_partitions_metis()
        self.convert_to_star()
        self.period += 1
        self.reset_node_weights()
        self.fade_edges()
        self.update_prev_shards()
        
        return transaction_buffer
    
    def printStats(self):
        return [self.migrations, self.expensive_migrations, self.avoidable_expensive_migrations]

    def updatePolicy_reactive(self, from_n, to_n):
        return None

    def apply_tx_reactive(self, from_n, to_n, migration):
        return
