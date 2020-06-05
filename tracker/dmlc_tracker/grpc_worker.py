'''
Script run by worker machines to start listening for RPCs.
gRPC Worker API

usage - python3 grpc_worker.py <PORT> <PATH_TO_DATA> <PATH_TO_MODEL>
'''
from concurrent import futures

import _credentials

import fxgb_pb2
import fxgb_pb2_grpc
import grpc

import ctypes
import sys

import pandas as pd
import xgboost as xgb


def get_dmlc_vars(env):
    '''
    Returns list of strings representing DMLC variables needed for rabit.
    Parsed in allreduce_base.cc from '<name>=<value>' format.
    
    Param:
        env - Env protobuf
    
    Return:
        list containing DMLC variables
    '''
    temp = [
        'DMLC_TRACKER_URI=' + env.DMLC_TRACKER_URI,
        'DMLC_TRACKER_PORT=' + str(env.DMLC_TRACKER_PORT),
        'DMLC_ROLE=' + env.DMLC_ROLE,
        'DMLC_NODE_HOST=' + env.DMLC_NODE_HOST,
        'DMLC_NUM_WORKER=' + str(env.DMLC_NUM_WORKER),
        'DMLC_NUM_SERVER=' + str(env.DMLC_NUM_SERVER),
    ]
    # Python strings are unicode, but C strings are bytes, so we must convert to bytes.
    return [bytes(s, 'utf-8') for s in temp]

def get_train_request_field_or_none(train_request, field):
    return getattr(train_request, field) if train_request.HasField(field) else None

def get_train_params(train_request):
    '''
    Returns (param, num_round) from parsing TrainRequest protobuf.
    '''
    fields = [
        'eta',
        'gamma',
        'max_depth',
        'min_child_weight',
        'max_delta_step',
        'subsample',
        'colsample_bytree',
        'colsample_bylevel',
        'colsample_bynode',
        'lambda',
        'alpha',
        'tree_method',
        'sketch_eps',
        'scale_pos_weight',
        'updater',
        'refresh_leaf', 
        'process_type', 
        'grow_policy', 
        'max_leaves',
        'max_bin',
        'predictor',
        'num_parallel_tree',
        'objective',
        'base_score',
        'eval_metric',
    ]
    param = {}
    for field in fields:
        val = get_train_request_field_or_none(train_request, field)
        if val:
            param[field] = val
    num_round = train_request.num_round if train_request.HasField('num_round') else 10
    return param, num_round

class FederatedXGBoostServicer():
    ''' gRPC servicer class which implements worker machine RPCs API. '''

    def __init__(self):
        self.dmlc_vars = None

    def Init(self, request, context):
        '''
        Initializes rabit and environment variables.
        When worker receives this RPC, it can accept or reject the federated training session.

        Params:
            init_request - InitRequest proto containing DMLC variables to set up node communication with tracker
            context - RPC context. Contains metadata about the connection

        Return:
            WorkerResponse proto (confirmation of initializatison success or failure).
        '''
        print('Request from aggregator [%s] to start federated training session:' % context.peer())
        accept_job = None
        while accept_job not in {'Y', 'N'}:
            print("Please enter 'Y' to confirm or 'N' to reject.")
            accept_job = input("Join session? [Y/N]: ")
        if accept_job == 'Y':
            self.dmlc_vars = get_dmlc_vars(request.dmlc_vars)
            return fxgb_pb2.WorkerResponse(success=True)
        else:
            return fxgb_pb2.WorkerResponse(success=False)

    def Train(self, request, context):
        '''
        Starts distributed training.

        Params:
            train_request - TrainRequest proto containing XGBoost parameters for training
            context - RPC context containing metadata about the connection

        Return:
            WorkerResponse proto (confirmation of training success or failure).
        '''
        try:
            print('Request from aggregator [%s] to start federated training session:' % context.peer())

            # Instantiate Federated XGBoost
            fed = xgb.Federated(self.dmlc_vars)
            print("rabit init")            
            # Get number of federating parties
            print(fed.get_num_parties())
            
            # Load training data
            # Ensure that each party's data is in the same location with the same name
            print("Load data")
            dtrain = fed.load_data("/home/ubuntu/federated-xgboost/demo/data/hb_train.csv")
            dval = fed.load_data("/home/ubuntu/federated-xgboost/demo/data/hb_val.csv")
            
            print('setting params')
            # Train a model
            params = {
                    "max_depth": 3, 
                    "min_child_weight": 1.0, 
                    "lambda": 1.0,
                    "tree_method": "hist",
                    "objective": "binary:logistic"
                    }
            
            num_rounds = 20
            print("Training")
            bst = xgb.train(params, dtrain, num_rounds, evals=[(dtrain, "dtrain"), (dval, "dval")])
            
            dtest = fed.load_data("/home/ubuntu/federated-xgboost/demo/data/hb_test.csv")
            
            # Get predictions
            print("Predicting")
            ypred = bst.predict(dtest)
            
            print("The first twenty predictions are: ", ypred[:20])
            
            # Save the model
            bst.save_model("sample_model.model")
            
            # Shutdown
            fed.shutdown()
            return fxgb_pb2.WorkerResponse(success=True)
        except:
            return fxgb_pb2.WorkerResponse(success=False)


# Start gRPC server listening on port 'port'
def start_worker(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    fxgb_pb2_grpc.add_FXGBWorkerServicer_to_server(FederatedXGBoostServicer(), server)
    server_credentials = grpc.ssl_server_credentials(
        ((_credentials.SERVER_CERTIFICATE_KEY, _credentials.SERVER_CERTIFICATE),))
    #  server.add_secure_port('[::]:' + port, server_credentials)
    server.add_insecure_port('[::]:' + port)
    print("Starting RPC server on port ", port)
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    assert len(sys.argv) == 2, "usage - python3 grpc_worker.py <PORT>" 
    start_worker(sys.argv[1])
