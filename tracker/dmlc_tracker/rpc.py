#!/usr/bin/env python3
'''
DMLC submission script by gRPC

../../../dmlc-core/tracker/dmlc-submit --num-workers 3 --cluster rpc --host-file hosts.config
'''
from multiprocessing import Pool, Process
import os, subprocess, logging
from threading import Thread
import sys

import grpc

sys.path.insert(0,'../../../dmlc-core/tracker/dmlc_tracker')
import fxgb_pb2
import fxgb_pb2_grpc
import tracker
import _credentials


def run(worker, command, env, password):
    '''
    This function is run by every thread (one thread is spawned per worker)

    Params:
        worker - string that is IP:PORT format
        command - script to run
        env - environment variables
    '''
    creds = grpc.ssl_channel_credentials(_credentials.ROOT_CERTIFICATE)
    with grpc.secure_channel(worker, creds) as channel:
        stub = fxgb_pb2_grpc.FXGBWorkerStub(channel)
        stub.Init(fxgb_pb2.InitRequest(env=env))
        stub.Train(fxgb_pb2.TrainRequest())


def submit(args):
    assert args.host_file is not None
    with open(args.host_file) as f:
        tmp = f.readlines()
    assert len(tmp) > 0
    hosts = [host.strip() for host in tmp if len(host.strip()) > 0]

    # When submit is called, the workers are assumed to have run 'grpc_worker.py'.
    def gRPC_submit(nworker, nserver, pass_envs):      
        password = input("Enter session password: ")  
        for i in range(nworker):
            worker = hosts[i]
            print('worker ip:port -', worker)

            # Package PASS ENVS into protobuf
            env = fxgb_pb2.Env(
                DMLC_TRACKER_URI=pass_envs['DMLC_TRACKER_URI'],
                DMLC_TRACKER_PORT=pass_envs['DMLC_TRACKER_PORT'],
                DMLC_ROLE='worker',
                DMLC_NODE_HOST=worker[:worker.index(':')],
                DMLC_NUM_WORKER=pass_envs['DMLC_NUM_WORKER'],
                DMLC_NUM_SERVER=pass_envs['DMLC_NUM_SERVER'],
            )

            # spawn thread to call RPC
            thread = Thread(target=run, args=(worker, 
                                            ' '.join(args.command), 
                                            env, 
                                            password
                                            ))
            thread.setDaemon(True)
            thread.start()

    tracker.submit(args.num_workers,
                   args.num_servers,
                   fun_submit=gRPC_submit,
                   hostIP=args.host_ip,
                )
