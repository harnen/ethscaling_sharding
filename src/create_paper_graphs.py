#!/usr/bin/python3
import pandas as pd
from matplotlib import pyplot as plt
from compare_policies import *
import time
import sys
import matplotlib

font = {'family' : 'normal',
        'weight' : 'bold',
        'size'   : 16}

matplotlib.rc('font', **font)
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

input_file = './input/multi_small.csv'
input_file = './input/data_hackfs-.csv'

intra_cost = 1
inter_cost = 2
buffer_ratio = 2
shards_num = 16
shard_capacity = 100
time_interval = 100


def restore_default():
    global intra_cost, inter_cost, buffer_ratio, shards_num, shard_capacity, time_interval
    intra_cost = 1
    inter_cost = 2
    shards_num = 2
    shard_capacity = 100
    buffer_ratio = 2
    time_interval = 100



results = {}
dump = []
counter = 0


def run():
    policy_list = []
    policy_list.append(HashPolicy(shards_num))
    policy_list.append(MichalPolicy(shards_num, intra_shard_cost=intra_cost, inter_shard_cost=inter_cost))
    #print("Running sliding windows policy with:", shards_num, intra_cost, inter_cost, 2*buffer_ratio*shard_capacity*shards_num )
    policy_list.append(SlidingWindowPolicy(shards_num, intra_shard_cost=intra_cost, inter_shard_cost=inter_cost, window_length=2*buffer_ratio*shard_capacity*shards_num))

    number_of_lines = len(open(input_file).readlines(  ))


    for policy in policy_list:
        #continue
        print("\n Running", policy.__class__.__name__ , " at ", time.time(), file = sys.stderr)
        result = evaluate_policy(input_file, buffer_ratio, shards_num, shard_capacity, time_interval, policy, inter_cost)
        statsList = policy.printStats()


        if isinstance(policy, SlidingWindowPolicy) or isinstance(policy, MichalPolicy):
            print('Migrations ordered (not necessarily completed migrations due to capacity): ', statsList[0])
            print('Load:', policy.shard_load)
        
        print(type(policy).__name__ + str(counter), "steps:", result['steps'], 
            "wasted_capacity:", sum(sum(result['wasted_capacity'], [])), 
            "inter", sum(sum(result['inter'], [])), 
            'intra', sum(sum(result['intra'], [])), 
            'load', sum(sum(result['load'], [])))
        result['wasted_capacity'] = sum(sum(result['wasted_capacity'], []))
        result["inter"] = sum(sum(result['inter'], []))
        result["intra"] = sum(sum(result['intra'], []))
        result["load"] = sum(sum(result['load'], []))
        result['policy'] = type(policy).__name__
        result['inter_cost'] = inter_cost
        result['buffer_ratio'] = buffer_ratio
        result['input_file'] = input_file
        result['shards_num'] = shards_num
        result['shard_capacity'] = shard_capacity
        result['num_of_tx'] = number_of_lines
        result['tps'] = result['num_of_tx']/(result['steps']*shard_capacity*shards_num)
        result['inter_ratio'] = result['inter']/(result['num_of_tx'])
        
            
        dump.append(result)


def run_all():
    global inter_cost, shards_num, shard_capacity, buffer_ratio
    restore_default()
    for i in [1, 2, 4, 6, 8, 10]:
        inter_cost = i
        run()

    restore_default()
    for i in [1, 2, 4, 10, 20, 30]:
        shards_num = i
        run()

    restore_default()
    for i in [100, 500, 1000]:
        shard_capacity = i
        run()

    restore_default()
    for i in [1, 2, 10]:
        buffer_ratio = i
        run()

    df = pd.DataFrame(dump)
    print(df)
    df.to_csv('dump.csv')
    print("#################################")

def select_results_with_default_params(df, exclude = None):
    restore_default()
    params = ['inter_cost', 'buffer_ratio', 'shards_num', 'shard_capacity']
    defaults = {}
    defaults['inter_cost'] = inter_cost
    defaults['buffer_ratio'] = buffer_ratio
    defaults['shards_num'] = shards_num
    defaults['shard_capacity'] = shard_capacity

    if(exclude != None):
        params.remove(exclude)
    for p in params:
        df = df.loc[df[p] == defaults[p]]
    return df

def plot_feature(ax, df, label, y, x_title, y_title):
    colors = ['0.1', '0.5', '0.8']
    styles = ['solid', 'dashed', 'dashdot']
    counter = 0
    for key, group in df.groupby('policy'):
        group_specific = select_results_with_default_params(group, label) 
        print(group_specific)
        ax.plot(group_specific[label], group_specific[y], label=key, linestyle = styles[counter%len(styles)], linewidth = 3, c=colors[counter%len(colors)])
        counter += 1
    ax.set_xlabel(x_title)
    ax.set_ylabel(y_title)
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.legend()

def analyze(input_file = 'dump.csv'):
    df = pd.read_csv(input_file)
    print(df)
    
    fig, ax = plt.subplots(figsize=(10, 4))
    plot_feature(ax, df, 'inter_cost', 'steps', 'Cross-shard tx cost', '#Blocks')
    fig, ax = plt.subplots(figsize=(10, 4))
    plot_feature(ax, df, 'shards_num', 'steps', '#Shards', '#Blocks')
    fig, ax = plt.subplots(figsize=(10, 4))
    plot_feature(ax, df, 'shard_capacity', 'steps', 'Shard capacity', '#Blocks')
    fig, ax = plt.subplots(figsize=(10, 4))
    plot_feature(ax, df, 'buffer_ratio', 'steps', 'Mempoll/blockchain capacity ratio', '#Blocks')
    
    fig, ax = plt.subplots(figsize=(10, 4))
    plot_feature(ax, df, 'inter_cost', 'tps', 'Cross-shard tx cost', 'Efficiency')
    fig, ax = plt.subplots(figsize=(10, 4))
    plot_feature(ax, df, 'shards_num', 'tps', '#shards', 'Efficiency')
    fig, ax = plt.subplots(figsize=(10, 4))
    plot_feature(ax, df, 'shard_capacity', 'tps', 'Shard capacity', 'Efficiency')
    fig, axes = plt.subplots(figsize=(10, 4))
    plot_feature(ax, df, 'buffer_ratio', 'tps', 'Mempoll/blockchain capacity ratio', 'Efficiency')
    
    fig, axes = plt.subplots(2, 2)
    plot_feature(axes[0, 0], df, 'inter_cost', 'intra', 'Cross-shard tx cost', 'cross-shard tx ratio')
    plot_feature(axes[0, 1], df, 'shards_num', 'intra', '#shards', 'cross-shard tx ratio')
    plot_feature(axes[1, 0], df, 'shard_capacity', 'intra', 'Shard capacity', 'cross-shard tx ratio')
    plot_feature(axes[1, 1], df, 'buffer_ratio', 'intra', 'Mempoll/blockchain capacity ratio', 'cross-shard tx ratio')

    plt.show()
    quit()
    fig, axes = plt.subplots(2, 2)
    for key, group in df.groupby('policy'):
        print(key)
        plot_feature(axes[0, 0], group, 'inter_cost', key, 'num_migrations', 'Cross-shard tx cost', '#Migrations')
        plot_feature(axes[0, 1], group, 'shards_num', key, 'num_migrations', '#shards', '#Migrations')
        plot_feature(axes[1, 0], group, 'shard_capacity', key, 'num_migrations', 'Shard capacity', '#Migrations')
        plot_feature(axes[1, 1], group, 'buffer_ratio', key, 'num_migrations', 'Mempoll/blockchain capacity ratio', '#Migrations')
    
    fig, axes = plt.subplots(2, 2)
    for key, group in df.groupby('policy'):
        print(key)
        plot_feature(axes[0, 0], group, 'inter_cost', key, 'wasted_capacity', 'Cross-shard tx cost', 'Wasted capacity')
        plot_feature(axes[0, 1], group, 'shards_num', key, 'wasted_capacity', '#shards', 'Wasted capacity')
        plot_feature(axes[1, 0], group, 'shard_capacity', key, 'wasted_capacity', 'Shard capacity', 'Wasted capacity')
        plot_feature(axes[1, 1], group, 'buffer_ratio', key, 'wasted_capacity', 'Mempoll/blockchain capacity ratio', 'Wasted capacity')

    fig, axes = plt.subplots(2, 2)
    for key, group in df.groupby('policy'):
        print(key)
        plot_feature(axes[0, 0], group, 'inter_cost', key, 'load', 'Cross-shard tx cost', 'Total load')
        plot_feature(axes[0, 1], group, 'shards_num', key, 'load', '#shards', 'Total load')
        plot_feature(axes[1, 0], group, 'shard_capacity', key, 'load', 'Shard capacity', 'Total load')
        plot_feature(axes[1, 1], group, 'buffer_ratio', key, 'load', 'Mempoll/blockchain capacity ratio', 'Total load')
    plt.show()

run_all()
analyze()
