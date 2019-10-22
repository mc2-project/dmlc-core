'''
Script run by worker machines to start listening for RPC.
'''

from concurrent import futures

import grpc

import fxgb_pb2
import fxgb_pb2_grpc

import shlex
import subprocess
import os

PORT = '50050'

def env_to_dict(env):
    '''
    changes Env protobuf into python dictionary
    '''
    retval = os.environ.copy()
    new_env = {
        'DMLC_TRACKER_URI' : env.DMLC_TRACKER_URI,
        'DMLC_TRACKER_PORT' : str(env.DMLC_TRACKER_PORT),
        'DMLC_ROLE' : env.DMLC_ROLE,
        'DMLC_NODE_HOST' : env.DMLC_NODE_HOST,
        'DMLC_NUM_WORKER' : str(env.DMLC_NUM_WORKER),
        'DMLC_NUM_SERVER' : str(env.DMLC_NUM_SERVER),
    }
    retval.update(new_env)
    return retval

class FederatedXGBoostServicer():
    def __init__(self):
        print("Started up FXGB worker. Now listening on port %s for RPC to start job." % PORT)

    def StartJob(self, request, context):
        # print(request)
        print('Request from aggregator to run this job:', request.cmd)
        accept_job = input("Run this job? [Y/N]: ")
        while accept_job not in {'Y', 'N'}:
            print("Please enter 'Y' to confirm or 'N' to reject.")
            accept_job = input("Run this job? [Y/N]: ")
        
        if accept_job == 'Y':
            args = shlex.split(request.cmd)
            env = env_to_dict(request.env)
            subprocess.Popen(args, env=env)
            return fxgb_pb2.WorkerResponse(accepted_job=True)
        else:
            return fxgb_pb2.WorkerResponse(accepted_job=False)


def start_worker():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    fxgb_pb2_grpc.add_FXGBWorkerServicer_to_server(FederatedXGBoostServicer(), server)
    server.add_insecure_port('[::]:' + PORT) # TODO: use SECURE port, add AUTHENTICATION
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    start_worker()
