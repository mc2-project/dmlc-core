#!/usr/bin/env python3
"""
DMLC submission script by gRPC
"""
from multiprocessing import Pool, Process
import os, subprocess, logging
from threading import Thread
from . import tracker

import sys
from . import fxgb_pb2
from . import fxgb_pb2_grpc

import grpc

def submit(args):
    assert args.host_file is not None
    with open(args.host_file) as f:
        tmp = f.readlines()
    assert len(tmp) > 0
    hosts = [host.strip() for host in tmp if len(host.strip()) > 0]
    
    '''
    Thread function that calls RPC

    Params:
        worker - string that is IP:PORT format
        command - script to run
        env - environment variables
    '''
    def run(worker, command, env):
        with grpc.insecure_channel(worker) as channel: #TODO: use secure port     
            stub = fxgb_pb2_grpc.FXGBWorkerStub(channel)
            stub.StartJob(fxgb_pb2.Job(cmd=command, env=env))

    # When submit is called, the workers are assumed to have run 'grpc_worker.py'.
    def gRPC_submit(nworker, nserver, pass_envs):
        # Parse out command.
        command = ' '.join(args.command)
        
        for i in range(nworker):
            worker = hosts[i]
            print('worker ip:port -', worker)

            # Package PASS ENVS into protobuf
            env = fxgb_pb2.Job.Env(
                DMLC_TRACKER_URI=pass_envs['DMLC_TRACKER_URI'],
                DMLC_TRACKER_PORT=pass_envs['DMLC_TRACKER_PORT'],
                DMLC_ROLE='worker',
                DMLC_NODE_HOST=worker[:worker.index(':')],
                DMLC_NUM_WORKER=pass_envs['DMLC_NUM_WORKER'],
                DMLC_NUM_SERVER=pass_envs['DMLC_NUM_SERVER'],
            )

            # spawn thread to call RPC
            thread = Thread(target=run, args=(worker,command,env))
            thread.setDaemon(True)
            thread.start()

    tracker.submit(args.num_workers,
                   args.num_servers,
                   fun_submit=gRPC_submit,
                   pscmd=(' '.join(args.command)),
                   hostIP=args.host_ip,
                )
